"""Build native artifacts and assemble an installable platform release."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import platform
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUTS = ROOT.parent / "outputs"
PYTHON_PACKAGE = ROOT / "clients/python"
NODE_PACKAGE = ROOT / "clients/typescript"
TARGETS = {
    "macos-arm64": (".dylib", "darwin", "arm64", "aarch64-apple-darwin"),
    "macos-x64": (".dylib", "darwin", "x64", "x86_64-apple-darwin"),
    "linux-arm64": (".so", "linux", "arm64", "aarch64-unknown-linux-gnu"),
    "linux-x64": (".so", "linux", "x64", "x86_64-unknown-linux-gnu"),
    "windows-x64": (".dll", "win32", "x64", "x86_64-pc-windows-msvc"),
}


def target(label: str | None = None) -> tuple[str, str, str, str]:
    """Return release label, Rust library suffix, npm OS and npm CPU."""
    if label is None:
        system, machine = platform.system(), platform.machine().lower()
        cpu = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "x64", "amd64": "x64"}.get(machine)
        os_label = {"Darwin": "macos", "Linux": "linux", "Windows": "windows"}.get(system)
        label = f"{os_label}-{cpu}"
    if label not in TARGETS:
        raise SystemExit(f"Unsupported build target: {label}")
    suffix, npm_os, npm_cpu, _ = TARGETS[label]
    return label, suffix, npm_os, npm_cpu


def run(*args: str, cwd: pathlib.Path = ROOT, env: dict[str, str] | None = None) -> None:
    if args[0] == "npm" and os.name == "nt":
        args = (os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c",
                shutil.which("npm.cmd") or "npm.cmd", *args[1:])
    print("+", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, env=env, check=True)


def macos_baseline(npm_cpu: str) -> tuple[int, int]:
    baseline = (11, 0) if npm_cpu == "arm64" else (10, 12)
    requested = os.environ.get("MACOSX_DEPLOYMENT_TARGET")
    if requested:
        try:
            parts = requested.split(".")
            if len(parts) > 3 or not all(part.isdigit() for part in parts):
                raise ValueError("invalid version")
            baseline = max(baseline, (int(parts[0]), int(parts[1]) if len(parts) > 1 else 0))
        except ValueError as error:
            raise SystemExit(f"Invalid MACOSX_DEPLOYMENT_TARGET: {requested}") from error
    return baseline


def wheel_platform(npm_cpu: str, npm_os: str, minimum: tuple[int, int] | None = None) -> str:
    if npm_os == "win32":
        return "win_amd64"
    if npm_os == "linux":
        # An audited builder may opt into its proven baseline; default is generic.
        arch = "aarch64" if npm_cpu == "arm64" else "x86_64"
        requested = os.environ.get("DEOOS_LINUX_PLATFORM", f"linux_{arch}")
        if requested not in (f"linux_{arch}", f"manylinux_2_28_{arch}"):
            raise SystemExit(f"Invalid DEOOS_LINUX_PLATFORM for {npm_cpu}: {requested}")
        return requested
    baseline = minimum or macos_baseline(npm_cpu)
    return f"macosx_{baseline[0]}_{baseline[1]}_{'arm64' if npm_cpu == 'arm64' else 'x86_64'}"


def binary_metadata(path: pathlib.Path) -> tuple[str, str, tuple[int, int] | None]:
    """Read 64-bit executable headers; reject fat/unknown/malformed artifacts."""
    data = path.read_bytes()
    try:
        if data[:4] == b"\xcf\xfa\xed\xfe":
            cpu, _, _, commands, size = struct.unpack_from("<IIIII", data, 4)
            arch = {0x01000007: "x64", 0x0100000C: "arm64"}[cpu]
            if 32 + size > len(data):
                raise ValueError("truncated Mach-O commands")
            position, minimum = 32, None
            for _ in range(commands):
                command, length = struct.unpack_from("<II", data, position)
                if length < 8 or position + length > 32 + size:
                    raise ValueError("invalid Mach-O command")
                if command in (0x24, 0x32):  # VERSION_MIN_MACOSX / BUILD_VERSION
                    offset = 8 if command == 0x24 else 12
                    required = 16 if command == 0x24 else 24
                    if length < required:
                        raise ValueError("truncated deployment target")
                    if command == 0x32 and struct.unpack_from("<I", data, position + 8)[0] != 1:
                        raise ValueError("non-macOS Mach-O")
                    version = struct.unpack_from("<I", data, position + offset)[0]
                    minimum = max(minimum or (0, 0), (version >> 16, (version >> 8) & 255))
                position += length
            if minimum is None:
                raise ValueError("missing macOS deployment target")
            return "Darwin", arch, minimum
        if data[:4] == b"\x7fELF":
            if len(data) < 64 or data[4:6] != b"\x02\x01":
                raise ValueError("expected little-endian 64-bit ELF")
            arch = {62: "x64", 183: "arm64"}[struct.unpack_from("<H", data, 18)[0]]
            return "Linux", arch, None
        if data[:2] == b"MZ":
            offset = struct.unpack_from("<I", data, 0x3C)[0]
            if data[offset:offset + 4] != b"PE\0\0":
                raise ValueError("missing PE signature")
            machine = struct.unpack_from("<H", data, offset + 4)[0]
            if struct.unpack_from("<H", data, offset + 24)[0] != 0x20B:
                raise ValueError("expected 64-bit PE")
            return "Windows", {0x8664: "x64"}[machine], None
    except (KeyError, struct.error, ValueError) as error:
        raise SystemExit(f"Invalid native artifact {path}: {error}") from error
    raise SystemExit(f"Unsupported native artifact format: {path}")


def verify_binary_architecture(path: pathlib.Path, system: str, expected: str) -> None:
    actual_system, actual_cpu, _ = binary_metadata(path)
    if (actual_system, actual_cpu) != (system, expected):
        raise SystemExit(f"{path} is {actual_system}/{actual_cpu}, expected {system}/{expected}")


def release_directory(label: str, explicit_target: bool) -> pathlib.Path:
    triple = TARGETS[label][3]
    return ROOT / "engine/target" / (triple if explicit_target else "") / "release"


def library_name(stem: str, suffix: str) -> str:
    return f"{'' if suffix == '.dll' else 'lib'}{stem}{suffix}"


def verify_release_architecture(suffix: str, npm_os: str, npm_cpu: str, server: pathlib.Path | None = None) -> tuple[int, int] | None:
    system = {"darwin": "Darwin", "linux": "Linux", "win32": "Windows"}[npm_os]
    server = server or ROOT / "engine/target/release" / ("deoos-engine.exe" if npm_os == "win32" else "deoos-engine")
    artifacts = [server, PYTHON_PACKAGE / "deoos/native" / library_name("deoos_engine", suffix), NODE_PACKAGE / "dist/native/deoos_node.node"]
    minimum = None
    for artifact in artifacts:
        if not artifact.is_file():
            raise SystemExit(f"Expected native artifact was not built: {artifact}")
        verify_binary_architecture(artifact, system, npm_cpu)
        deployment = binary_metadata(artifact)[2]
        if deployment:
            minimum = max(minimum or (0, 0), deployment)
    return minimum


def stage_native_artifacts(label: str, suffix: str, explicit_target: bool) -> None:
    engine_dir = release_directory(label, explicit_target)
    node_dir = ROOT / "bindings/node/target" / (TARGETS[label][3] if explicit_target else "") / "release"
    for source, destination in [
        (engine_dir / library_name("deoos_engine", suffix), PYTHON_PACKAGE / "deoos/native" / library_name("deoos_engine", suffix)),
        (node_dir / library_name("deoos_node", suffix), NODE_PACKAGE / "dist/native/deoos_node.node"),
    ]:
        if not source.is_file():
            raise SystemExit(f"Expected native artifact was not built: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Replace the inode rather than overwriting a library mapped by a live worker.
        # In-place writes can invalidate macOS code-signature pages in both old and new clients.
        with tempfile.TemporaryDirectory(dir=destination.parent) as directory:
            staged = pathlib.Path(directory) / destination.name
            shutil.copy2(source, staged)
            os.replace(staged, destination)

def build(label: str | None = None) -> tuple[str, str, str, str]:
    explicit_target = label is not None
    label, suffix, npm_os, npm_cpu = target(label)
    flags = ("--target", TARGETS[label][3]) if explicit_target else ()
    env = dict(os.environ)
    if npm_os == "darwin":
        env["MACOSX_DEPLOYMENT_TARGET"] = ".".join(map(str, macos_baseline(npm_cpu)))
    if npm_os == "win32":
        env["RUSTFLAGS"] = env.get("RUSTFLAGS", "") + " -C target-feature=+crt-static"
    run("cargo", "build", "--release", "--locked", "--manifest-path", "engine/Cargo.toml", *flags, env=env)
    run("cargo", "build", "--release", "--locked", "--manifest-path", "bindings/node/Cargo.toml", *flags, env=env)
    run("npm", "ci", "--prefix", "clients/typescript")
    run("npm", "run", "build", "--prefix", "clients/typescript")
    stage_native_artifacts(label, suffix, explicit_target)
    engine_dir = release_directory(label, explicit_target)
    verify_release_architecture(suffix, npm_os, npm_cpu, engine_dir / ("deoos-engine.exe" if npm_os == "win32" else "deoos-engine"))
    return label, suffix, npm_os, npm_cpu


def package(label: str, suffix: str, npm_os: str, npm_cpu: str, explicit_target: bool = False) -> pathlib.Path:
    version = json.loads((NODE_PACKAGE / "package.json").read_text())["version"]
    release = OUTPUTS / f"deoos-{version}-{label}"
    server = release_directory(label, explicit_target) / ("deoos-engine.exe" if npm_os == "win32" else "deoos-engine")
    stage_native_artifacts(label, suffix, explicit_target)
    minimum = verify_release_architecture(suffix, npm_os, npm_cpu, server)
    if release.exists():
        shutil.rmtree(release)
    python_out, node_out = release / "python", release / "node"
    (release / "bin").mkdir(parents=True)
    python_out.mkdir()
    node_out.mkdir()
    shutil.copy2(server, release / "bin" / ("deoos-server.exe" if npm_os == "win32" else "deoos-server"))
    shutil.copy2(ROOT / "README.md", release / "README.md")
    shutil.copy2(ROOT / "LICENSE", release / "LICENSE")
    shutil.copy2(ROOT / "compose.yaml", release / "compose.yaml")
    examples = release / "examples"
    examples.mkdir()
    for name in ("README.md", "library_python.py", "library_typescript.mjs", "server_python.py", "server_typescript.mjs", "workflow_python.py", "workflow_typescript.mjs", "use_cases.py", "use_cases.mjs", "hacker_news.py", "hacker_news.mjs", "hacker-news", "hacker_news_demo.py"):
        shutil.copy2(ROOT / "examples" / name, examples / name)
    # Isolate the selected native artifact from cached libraries for other platforms.
    with tempfile.TemporaryDirectory(prefix="deoos-python-") as temp:
        stage = pathlib.Path(temp)
        for name in ("pyproject.toml", "setup.py"):
            shutil.copy2(PYTHON_PACKAGE / name, stage / name)
        shutil.copy2(ROOT / "LICENSE", stage / "LICENSE")
        shutil.copytree(PYTHON_PACKAGE / "deoos", stage / "deoos", ignore=shutil.ignore_patterns("native", "__pycache__", "*.pyc"))
        native = stage / "deoos/native"
        native.mkdir()
        name = library_name("deoos_engine", suffix)
        shutil.copy2(PYTHON_PACKAGE / "deoos/native" / name, native / name)
        wheel_env = dict(os.environ, DEOOS_WHEEL_PLATFORM=wheel_platform(npm_cpu, npm_os, minimum))
        run(sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", str(python_out), str(stage), env=wheel_env)
    wheels = list(python_out.glob("*.whl"))
    if len(wheels) != 1 or not wheels[0].name.startswith("deoos-") or "-py3-none-" not in wheels[0].name:
        raise SystemExit(f"Expected one py3 platform wheel, found: {[p.name for p in wheels]}")
    with tempfile.TemporaryDirectory(prefix="deoos-npm-") as temp:
        stage = pathlib.Path(temp)
        shutil.copytree(NODE_PACKAGE / "dist", stage / "dist")
        shutil.copy2(ROOT / "LICENSE", stage / "LICENSE")
        manifest = json.loads((NODE_PACKAGE / "package.json").read_text())
        manifest["os"], manifest["cpu"] = [npm_os], [npm_cpu]
        (stage / "package.json").write_text(json.dumps(manifest, indent=2) + "\n")
        run("npm", "pack", "--pack-destination", str(node_out), cwd=stage)
    npm_packages = list(node_out.glob("*.tgz"))
    if len(npm_packages) != 1:
        raise SystemExit(f"Expected one npm tarball, found: {[p.name for p in npm_packages]}")
    files = sorted(p for p in release.rglob("*") if p.is_file())
    (release / "SHA256SUMS").write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.relative_to(release).as_posix() + "\n" for path in files))
    with tarfile.open(OUTPUTS / f"{release.name}.tar.gz", "w:gz") as archive:
        archive.add(release, arcname=release.name)
    print(json.dumps({"release": str(release), "target": label, "python_wheel": [p.name for p in wheels], "npm_package": [p.name for p in npm_packages]}, indent=2))
    return release


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=tuple(TARGETS), help="build/package a Rust cross-target (toolchain/linker must already be installed)")
    parser.add_argument("--build-only", action="store_true", help="build native libraries and TypeScript output")
    parser.add_argument("--package-only", action="store_true", help="package already-built artifacts")
    args = parser.parse_args()
    if sys.version_info < (3, 10):
        raise SystemExit(f"Python 3.10 or newer is required for packaging; found {platform.python_version()}")
    if args.build_only and args.package_only:
        parser.error("choose at most one of --build-only and --package-only")
    info = target(args.target)
    if not args.package_only:
        info = build(args.target)
    if not args.build_only:
        package(*info, explicit_target=args.target is not None)


if __name__ == "__main__":
    main()
