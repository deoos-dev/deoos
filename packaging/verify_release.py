"""Fail closed unless all five release packages have matching fresh-install evidence.

This is a local pre-publication check; it neither builds nor uploads anything.
Build attestations are JSON files named build-<target>.json in --evidence, with
source_commit, target, archive_sha256. Package-smoke reports use the existing
tests/package_smoke.py schema. Cross-compilation alone cannot satisfy this gate.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import pathlib
import re
import tarfile
import tempfile
import zipfile

from build_package import TARGETS, binary_metadata, library_name


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def tar_files(data: bytes) -> dict[str, bytes]:
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for entry in archive:
            name = pathlib.PurePosixPath(entry.name)
            require(not name.is_absolute() and ".." not in name.parts, "Unsafe archive path")
            require(entry.isfile() or entry.isdir(), f"Unexpected archive entry: {entry.name}")
            if entry.isfile():
                require(entry.name not in files, f"Duplicate archive entry: {entry.name}")
                files[entry.name] = archive.extractfile(entry).read()
    return files


def architecture(data: bytes, system: str, cpu: str) -> None:
    with tempfile.TemporaryDirectory(prefix="deoos-header-") as directory:
        artifact = pathlib.Path(directory) / "artifact"
        artifact.write_bytes(data)
        actual_system, actual_cpu, _ = binary_metadata(artifact)
    require((actual_system, actual_cpu) == (system, cpu),
            f"Native header is {actual_system}/{actual_cpu}, expected {system}/{cpu}")


def verify_target(outputs: pathlib.Path, evidence: pathlib.Path, version: str,
                  source: str, target: str) -> dict:
    label = f"deoos-{version}-{target}"
    archive_path = outputs / f"{label}.tar.gz"
    archive_bytes = archive_path.read_bytes()
    provenance = json.loads((evidence / f"build-{target}.json").read_text())
    require(provenance.get("source_commit") == source and provenance.get("target") == target,
            f"{target}: build source/target does not match")
    require(provenance.get("archive_sha256") == digest(archive_bytes),
            f"{target}: build attestation archive hash does not match")
    wrapped = tar_files(archive_bytes)
    require(all(path.startswith(label + "/") for path in wrapped), f"{target}: unexpected archive root")
    files = {path[len(label) + 1:]: data for path, data in wrapped.items()}
    sums = {}
    for line in files["SHA256SUMS"].decode().splitlines():
        checksum, path = line.split("  ", 1)
        require(path not in sums and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None,
                f"{target}: invalid or duplicate checksum")
        sums[path] = checksum
    hashes = {path: digest(data) for path, data in files.items() if path != "SHA256SUMS"}
    require(sums == hashes, f"{target}: incomplete or incorrect payload checksums")
    for path in ("LICENSE", "README.md",
                 "examples/README.md", "examples/library_python.py", "examples/library_typescript.mjs",
                 "examples/server_python.py", "examples/server_typescript.mjs"):
        require(bool(files.get(path)), f"{target}: missing {path}")
    require(b"Apache License" in files["LICENSE"], f"{target}: missing Apache license text")
    require(not any(path.endswith((".rs", "Cargo.toml", "Cargo.lock")) for path in files),
            f"{target}: unexpected Rust source in distribution")
    suffix, npm_os, cpu, _ = TARGETS[target]
    system = {"darwin": "Darwin", "linux": "Linux", "win32": "Windows"}[npm_os]
    server = "bin/deoos-server" + (".exe" if npm_os == "win32" else "")
    architecture(files[server], system, cpu)
    wheels = [data for path, data in files.items() if path.startswith("python/") and path.endswith(".whl")]
    nodes = [data for path, data in files.items() if path.startswith("node/") and path.endswith(".tgz")]
    require(len(wheels) == len(nodes) == 1, f"{target}: expected one wheel and npm package")
    with zipfile.ZipFile(io.BytesIO(wheels[0])) as wheel:
        require(len(wheel.namelist()) == len(set(wheel.namelist())), f"{target}: duplicate wheel entry")
        python = {name: wheel.read(name) for name in wheel.namelist() if not name.endswith("/")}
    sdk_version = version.split("-", 1)[0]
    metadata = [data.decode() for name, data in python.items() if name.endswith(".dist-info/METADATA")]
    require(len(metadata) == 1 and f"\nName: deoos\n" in metadata[0]
            and f"\nVersion: {sdk_version}\n" in metadata[0]
            and "License-Expression: Apache-2.0" in metadata[0], f"{target}: wheel metadata mismatch")
    native_path = "deoos/native/" + library_name("deoos_engine", suffix)
    require([name for name in python if name.startswith("deoos/native/")] == [native_path],
            f"{target}: wheel has missing or extra native libraries")
    architecture(python[native_path], system, cpu)
    require(any(name.endswith("/LICENSE") and data == files["LICENSE"] for name, data in python.items()),
            f"{target}: wheel license missing or differs")
    node = tar_files(nodes[0])
    manifest = json.loads(node["package/package.json"])
    require(manifest.get("name") == "deoos" and manifest.get("version") == sdk_version and manifest.get("os") == [npm_os]
            and manifest.get("cpu") == [cpu] and manifest.get("license") == "Apache-2.0",
            f"{target}: npm metadata mismatch")
    require(node.get("package/LICENSE") == files["LICENSE"], f"{target}: npm license mismatch")
    require(bool(node.get("package/dist/index.d.ts")), f"{target}: missing TypeScript types")
    architecture(node["package/dist/native/deoos_node.node"], system, cpu)
    installed = {"python_sdk": digest(python["deoos/__init__.py"]),
                 "python_native": digest(python[native_path]),
                 "typescript_sdk": digest(node["package/dist/index.js"]),
                 "typescript_native": digest(node["package/dist/native/deoos_node.node"])}
    candidates = sorted(evidence.glob(f"package-smoke-{label}-*.json"))
    errors = []
    for path in candidates:
        try:
            report = json.loads(path.read_text())
            require(report.get("release_hashes") == hashes, "tested payload differs from archive")
            require(report.get("installed_hashes") == installed, "installed SDK/native hashes differ")
            require(report.get("server_version") == f"deoos-engine {sdk_version}", "server version differs")
            require(report.get("cleaned") is True and not report.get("cleanup_errors")
                    and not report.get("error"), "smoke failed or cleanup incomplete")
            expected = {(mode, language) for mode in ("library", "server") for language in ("python", "typescript")}
            require(len(report.get("modes", [])) == 4 and
                    {(cell["mode"], cell["worker"]) for cell in report["modes"]} == expected,
                    "missing workflow mode/SDK coverage")
            require(len(report.get("worker_api", [])) == 4 and
                    {(cell["mode"], cell["language"]) for cell in report["worker_api"]} == expected,
                    "missing worker API mode/SDK coverage")
            for cell in report["modes"]:
                require(cell.get("completed_attempts", 0) >= 1 and cell.get("output") is not None,
                        "workflow did not complete")
                require(cell.get("resume_worker") in ("python", "typescript")
                        and cell["resume_worker"] != cell["worker"], "missing cross-language recovery")
                if cell["mode"] == "server":
                    require(cell.get("server_restarted") is True, "missing server restart recovery")
            for cell in report["worker_api"]:
                checks = {
                    "already-stopped worker does not claim",
                    "idle stop interrupts long poll interval",
                    "active stop drains checkpoint and completion without another claim",
                    "default and callback errors propagate after recorded failure",
                    "failed ownership write has no recorded task failure ID",
                    "manual retry keeps historical failure summary",
                    "explicit continue recovers and retains historical failure",
                    "stable signal operation ID replays and rejects a different value",
                    "queued/waiting/assigned/completed summaries omit payloads and tokens",
                    "installed operator CLI JSON summary and human explanation omit payloads",
                }
                if cell["language"] == "python":
                    checks.add("simple CLI inspect; explicit internal details; separate history")
                if cell["mode"] == "server":
                    checks.add("authentication failure has no recorded task failure ID")
                require(checks <= set(cell.get("checks", [])), "worker API checks missing")
            for mode in ("library", "server"):
                runtime = report["runtime"][mode]
                machine = runtime["python"]["machine"].lower()
                py_cpu = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "x64", "amd64": "x64"}.get(machine)
                require((runtime["python"]["system"], py_cpu) == (system, cpu), "Python runtime target mismatch")
                require((runtime["node"]["platform"], runtime["node"]["arch"]) == (npm_os, cpu), "Node runtime target mismatch")
                for language in ("python", "node"):
                    process = runtime[language]
                    require(isinstance(process.get("client_pid"), int) and process["client_pid"] > 0
                            and isinstance(process.get("engine_pid"), int) and process["engine_pid"] > 0,
                            "Missing runtime process identity")
                    require((process["client_pid"] == process["engine_pid"]) == (mode == "library"),
                            "Runtime process identity does not match execution mode")
                if mode == "server":
                    require(runtime["python"]["engine_pid"] == runtime["node"]["engine_pid"],
                            "Server-mode SDKs did not use the same engine process")
            return {"target": target, "archive_sha256": digest(archive_bytes),
                    "evidence": str(path), "evidence_sha256": digest(path.read_bytes())}
        except (ValueError, KeyError, TypeError) as error:
            errors.append(f"{path.name}: {error}")
    raise ValueError(f"{target}: no matching successful fresh-install report; " + "; ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="release archive version, including alpha suffix")
    parser.add_argument("--source", required=True, help="full source commit common to all builds")
    parser.add_argument("--outputs", type=pathlib.Path, required=True)
    parser.add_argument("--evidence", type=pathlib.Path, required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.source) is not None, "Source must be a full commit SHA")
    results, failures = [], []
    for target in TARGETS:
        try:
            results.append(verify_target(args.outputs, args.evidence, args.version, args.source, target))
        except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, zipfile.BadZipFile, SystemExit) as error:
            failures.append(f"{target}: {error}")
    print(json.dumps({"complete": not failures, "source_commit": args.source,
                      "targets": results, "failures": failures}, indent=2))
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
