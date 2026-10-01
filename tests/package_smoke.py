"""Fresh-install acceptance for a packaged Python/TypeScript workflow."""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
import venv
import zipfile

import boto3
from botocore.exceptions import ClientError

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAX_SAFE_INTEGER = 9_007_199_254_740_991


def command(args, *, cwd=None, env=None, timeout=90, check=True):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True,
                            timeout=timeout)
    if check and result.returncode:
        raise AssertionError(f"command failed ({result.returncode}): {args}\n"
                             f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, path)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def npm_args(executable, args):
    if os.name == "nt":
        return [os.environ.get("COMSPEC", "cmd.exe"), "/c", executable, *args]
    return [executable, *args]


def wait_server(process, url):
    for _ in range(100):
        if process.poll() is not None:
            raise AssertionError(f"server exited early ({process.returncode})")
        try:
            with urllib.request.urlopen(url + "/health", timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(.1)
    raise AssertionError("server did not become healthy")


def stop_process(process, label):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
    if process.poll() is None:
        raise AssertionError(f"{label} is still running")


def inspect(python, script, task_id, env, cwd):
    result = command([python, str(script), "inspect", "--id", task_id],
                     env=env, cwd=cwd)
    return json.loads(result.stdout)


def runtime_info(mode, python, node, env, cwd, server_pid):
    python_probe = (
        "import json,os,platform,sys; from deoos import Client; "
        "c=Client.remote(os.environ['ENGINE_URL'],os.environ.get('ENGINE_TOKEN')) "
        "if os.environ['DEOOS_MODE']=='server' else Client(bucket=os.environ['AWS_BUCKET']); "
        "i=c.request('/info'); print(json.dumps({'system':platform.system(),'machine':platform.machine(),"
        "'version':platform.python_version(),'executable':sys.executable,'client_pid':os.getpid(),"
        "'engine_pid':i['process_id']})); c.close()"
    )
    py = json.loads(command([python, "-c", python_probe], cwd=cwd, env=env).stdout)
    node_probe = (
        "import {Client} from 'deoos'; const c=process.env.DEOOS_MODE==='server' "
        "?Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN) "
        ":new Client({bucket:process.env.AWS_BUCKET}); const i=await c.request('/info'); "
        "console.log(JSON.stringify({platform:process.platform,arch:process.arch,"
        "version:process.version,client_pid:process.pid,engine_pid:i.process_id}));"
    )
    js = json.loads(command([node, "--input-type=module", "-e", node_probe],
                            cwd=cwd, env=env).stdout)
    if mode == "library":
        assert py["client_pid"] == py["engine_pid"]
        assert js["client_pid"] == js["engine_pid"]
    else:
        assert py["engine_pid"] == server_pid and py["client_pid"] != server_pid
        assert js["engine_pid"] == server_pid and js["client_pid"] != server_pid
    return {"python": py, "node": js}


def installed_artifacts(python, node_modules, env):
    package = node_modules / "deoos"
    native = next((path for path in (package / "dist" / "native").glob("*.node")), None)
    sdk = package / "dist" / "index.js"
    python_native_name = {"nt": "deoos_engine.dll", "darwin": "libdeoos_engine.dylib",
                          "linux": "libdeoos_engine.so"}[os.name if os.name == "nt" else sys.platform]
    clean_env = {key: value for key, value in env.items() if key not in {
        "PYTHONPATH", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY",
    }}
    location = pathlib.Path(command(
        [python, "-c", "import deoos,pathlib; print(pathlib.Path(deoos.__file__).parent)"],
        cwd=node_modules.parent, env=clean_env, timeout=30).stdout.strip())
    native_files = list((location / "native").glob("*"))
    py_native = location / "native" / python_native_name
    assert native and sdk.is_file() and py_native.is_file()
    assert [path.name for path in native_files if path.is_file()] == [python_native_name]
    return {"python_sdk": sha256(location / "__init__.py"),
            "python_native": sha256(py_native), "typescript_sdk": sha256(sdk),
            "typescript_native": sha256(native)}


def run_workflow_case(mode, first_language, resume_language, python, node,
                      examples, package, root_env, processes, runtime_reports):
    prefix = f"smoke-{mode}-{first_language}-{uuid.uuid4().hex[:8]}"
    env = dict(root_env, DEOOS_MODE=mode, EXECUTION_PREFIX=prefix)
    if mode == "server":
        listen_port = port()
        env.update(ENGINE_BIND=f"127.0.0.1:{listen_port}",
                   ENGINE_URL=f"http://127.0.0.1:{listen_port}")
        env["ENGINE_TOKEN"] = "package-smoke-token"
    else:
        for key in ("ENGINE_BIND", "ENGINE_URL", "ENGINE_TOKEN"):
            env.pop(key, None)
    worker_env = dict(env)
    if mode == "server":
        for key in list(worker_env):
            if key.startswith("AWS_") or key in {
                "EXECUTION_PREFIX", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY",
            }:
                worker_env.pop(key)
        assert not any(key.startswith("AWS_") for key in worker_env)

    server = None

    def start_server():
        nonlocal server
        binary = package / "bin" / ("deoos-server.exe" if os.name == "nt" else "deoos-server")
        server = subprocess.Popen([str(binary)], cwd=package, env=env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append((server, "server"))
        wait_server(server, env["ENGINE_URL"])

    def cli(language, action, *args, timeout=90):
        executable = python if language == "python" else node
        script = examples / ("workflow_python.py" if language == "python" else "workflow_typescript.mjs")
        return command([executable, str(script), action, *args], cwd=examples,
                       env=worker_env, timeout=timeout)

    if mode == "server":
        start_server()
    if first_language == "python":
        runtime_reports[mode] = runtime_info(mode, python, node, worker_env, examples,
                                              server.pid if server else None)
    failed_id = f"{prefix}-failed"
    valid_id = f"{prefix}-valid"
    if first_language == "python":
        simple_examples = {
            "python": examples / f"{mode}_python.py",
            "typescript": examples / f"{mode}_typescript.mjs",
        }
        for language, script in simple_examples.items():
            executable = python if language == "python" else node
            result = command([executable, str(script), f"hello-{mode}-{language}"],
                             cwd=examples, env=worker_env)
            assert "Hello, World!" in result.stdout, result.stdout
    cli(first_language, "submit", "--id", failed_id, "--quantity", "2",
        "--unit-price", str(MAX_SAFE_INTEGER), "--delay-ms", "0")
    cli(first_language, "submit", "--id", valid_id, "--quantity", "2",
        "--unit-price", "25", "--delay-ms", "500")

    executable = python if first_language == "python" else node
    script = examples / ("workflow_python.py" if first_language == "python" else "workflow_typescript.mjs")
    worker = subprocess.Popen([executable, str(script), "work"], cwd=examples,
                              env=worker_env, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
    processes.append((worker, f"{first_language} worker"))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        failed = inspect(python, examples / "workflow_python.py", failed_id,
                         worker_env, examples)
        valid = inspect(python, examples / "workflow_python.py", valid_id,
                        worker_env, examples)
        assert worker.poll() is None, f"{first_language} worker stopped after failed order"
        if (failed["status"] == "failed" and valid["status"] == "waiting"
                and valid.get("waiting_on") == {"kind": "signal", "name": "approved"}
                and "cooldown" in valid["steps"]):
            break
        time.sleep(.25)
    else:
        raise AssertionError(f"{first_language} worker did not reach waiting signal state")
    stop_process(worker, f"{first_language} worker")
    assert failed["attempts"] == 1 and valid["attempts"] == 1

    if mode == "server":
        old_server = server
        stop_process(old_server, "shared server")
        start_server()
        assert server.pid != old_server.pid
        valid = inspect(python, examples / "workflow_python.py", valid_id,
                        worker_env, examples)
        assert valid["status"] == "waiting", "server restart lost the waiting task"

    cli(first_language, "approve", "--id", valid_id)
    cli(resume_language, "work", "--once")
    result = inspect(python, examples / "workflow_python.py", valid_id,
                     worker_env, examples)
    assert result["status"] == "completed", result
    assert result["attempts"] == 1, result
    assert result["output"] == {"order_id": valid_id, "total": 50, "approved": True}, result
    expected = {"validate", "price", "ready", "cooldown", "approved", "finalize"}
    assert expected <= set(result["definitions"]), result.get("definitions")
    assert expected <= set(result["steps"]), result.get("steps")
    if server is not None:
        stop_process(server, "shared server")
    return {"mode": mode, "worker": first_language, "resume_worker": resume_language,
            "failed_attempts": failed["attempts"], "completed_attempts": result["attempts"],
            "output": result["output"], "definitions": sorted(expected),
            "server_restarted": mode == "server"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", nargs="?", type=pathlib.Path,
                        help="release directory (defaults to this host's packaged release)")
    parser.add_argument("--backend", choices=("rustfs", "aws"), default="rustfs")
    parser.add_argument("--aws-profile", default=os.environ.get("AWS_PROFILE", "auto"),
                        help="AWS profile, or 'auto' for environment/instance credentials")
    args = parser.parse_args()

    if args.release:
        package = args.release.resolve()
    else:
        sys.path.insert(0, str(ROOT / "packaging"))
        from build_package import target
        version = json.loads((ROOT / "clients/typescript/package.json").read_text())["version"]
        package = ROOT.parent / "outputs" / f"deoos-{version}-{target()[0]}"
    assert package.is_dir(), f"release directory not found: {package}"
    sums = package / "SHA256SUMS"
    release_hashes = {}
    for line in sums.read_text().splitlines():
        digest, relative = line.split("  ", 1)
        path = package / relative
        assert path.is_file() and sha256(path) == digest, relative
        release_hashes[relative] = digest
    wheel, = (package / "python").glob("*.whl")
    npm_package, = (package / "node").glob("*.tgz")
    assert "-py3-none-" in wheel.name and not wheel.name.endswith("-any.whl")
    python_native_name = {"nt": "deoos_engine.dll", "darwin": "libdeoos_engine.dylib",
                          "linux": "libdeoos_engine.so"}[os.name if os.name == "nt" else sys.platform]
    with zipfile.ZipFile(wheel) as archive:
        native_names = [pathlib.PurePosixPath(name).name for name in archive.namelist()
                        if "/native/" in name]
        assert native_names == [python_native_name], native_names
    examples = package / "examples"
    server_binary = package / "bin" / ("deoos-server.exe" if os.name == "nt" else "deoos-server")
    server_version = command([str(server_binary), "--version"], cwd=package).stdout.strip()
    for name in ("workflow_python.py", "workflow_typescript.mjs", "library_python.py",
                 "library_typescript.mjs", "server_python.py", "server_typescript.mjs"):
        assert (examples / name).is_file(), f"missing packaged workflow example: {name}"

    bucket = os.environ.get("DEOOS_TEST_BUCKET_PREFIX", "deoos-smoke-") + uuid.uuid4().hex[:20]
    assert len(bucket) <= 63, "test bucket prefix is too long"
    caller_identity = None
    env = dict(os.environ)
    for key in ("AWS_SESSION_TOKEN", "PYTHONPATH", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY"):
        env.pop(key, None)
    if args.backend == "aws":
        expected_account = os.environ.get("DEOOS_EXPECTED_AWS_ACCOUNT")
        if expected_account is not None and (len(expected_account) != 12 or
                                              not expected_account.isascii() or
                                              not expected_account.isdigit()):
            raise ValueError("DEOOS_EXPECTED_AWS_ACCOUNT must be a 12-digit account ID")
        session = boto3.Session(profile_name=None if args.aws_profile == "auto" else args.aws_profile,
                                region_name="us-east-1")
        s3 = session.client("s3")
        caller_identity = session.client("sts").get_caller_identity()
        if expected_account:
            assert caller_identity["Account"] == expected_account, "unexpected AWS account"
        credentials = session.get_credentials().get_frozen_credentials()
        env.update(AWS_ACCESS_KEY_ID=credentials.access_key,
                   AWS_SECRET_ACCESS_KEY=credentials.secret_key, AWS_REGION="us-east-1")
        if credentials.token:
            env["AWS_SESSION_TOKEN"] = credentials.token
        env.pop("AWS_ENDPOINT", None)
        env.pop("AWS_ALLOW_HTTP", None)
    else:
        env.update(AWS_ACCESS_KEY_ID="local-development",
                   AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                   AWS_REGION="us-east-1", AWS_ALLOW_HTTP="true")
        env.pop("AWS_SESSION_TOKEN", None)
        env["AWS_ENDPOINT"] = os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:19000")
        s3 = boto3.client("s3", endpoint_url=env["AWS_ENDPOINT"], region_name="us-east-1",
                          aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"])
    env["AWS_BUCKET"] = bucket
    report = {"backend": args.backend, "release": str(package), "release_hashes": release_hashes,
              "server_version": server_version, "modes": [], "cleaned": False,
              "cleanup_errors": [], "bucket": bucket,
              "aws_identity": {"account": caller_identity["Account"], "arn": caller_identity["Arn"]}
                              if caller_identity else None}
    report_path = ROOT.parent / "outputs" / "evidence" / f"package-smoke-{package.name}-{args.backend}.json"
    # Persist the exact creation intent so CI can clean a timed-out/cancelled test.
    write_report(report_path, report)
    processes = []
    bucket_created = False
    try:
        s3.create_bucket(Bucket=bucket)
        bucket_created = True
        if args.backend == "aws":
            s3.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={key: True for key in
                    ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")},
            )
        with tempfile.TemporaryDirectory(prefix="deoos-package-smoke-") as scratch:
            work = pathlib.Path(scratch)
            venv.EnvBuilder(with_pip=True, symlinks=os.name != "nt").create(work / "venv")
            python = str(work / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
            npm = shutil.which("npm")
            node = shutil.which("node")
            assert npm and node, "Node.js and npm are required"
            command([python, "-m", "pip", "install", "--no-index", str(wheel)], timeout=120)
            (work / "package.json").write_text('{"private":true,"type":"module"}\n')
            command(npm_args(npm, ["install", "--offline", "--ignore-scripts", "--no-audit",
                                   "--no-fund", str(npm_package)]), cwd=work, timeout=120)
            work_examples = work / "examples"
            work_examples.mkdir()
            for example in examples.iterdir():
                if example.is_file():
                    shutil.copy2(example, work_examples / example.name)
            installed_hashes = installed_artifacts(python, work / "node_modules", env)
            report["installed_hashes"] = installed_hashes
            report["example_hashes"] = {path.name: sha256(path) for path in examples.iterdir()
                                        if path.is_file()}
            try:
                report["runtime"] = {}
                for mode in ("library", "server"):
                    for first, second in (("python", "typescript"), ("typescript", "python")):
                        report["modes"].append(run_workflow_case(
                            mode, first, second, python, node, work_examples, package,
                            env, processes, report["runtime"],
                        ))
            finally:
                for process, label in reversed(processes):
                    try:
                        stop_process(process, label)
                    except Exception as error:
                        report["cleanup_errors"].append(f"{label}: {error}")
    except BaseException as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        for process, label in reversed(processes):
            try:
                stop_process(process, label)
            except Exception as error:
                report["cleanup_errors"].append(f"{label}: {error}")
        if bucket_created:
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if objects:
                        deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                        if deleted.get("Errors"):
                            raise AssertionError(deleted["Errors"])
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    code = str(error.response.get("Error", {}).get("Code", ""))
                    assert code in ("404", "NoSuchBucket", "NotFound"), error
                else:
                    raise AssertionError("test bucket still exists after deletion")
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_errors"].append(f"bucket: {error}")
        write_report(report_path, report)
    assert report["cleaned"] and not report["cleanup_errors"], report
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
