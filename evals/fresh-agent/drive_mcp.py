"""Drive the MCP server through a real crash-resume workflow.

Starts deoos-server on local RustFS, speaks MCP 2025-11-25 over stdio, submits
survive, kills the worker inside hold, and reads the run back through MCP.
Prints the transcript. No mocks.
"""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "evals" / "fresh-agent"))
sys.path.insert(0, str(ROOT / "clients" / "python"))
from harness import Engine  # noqa: E402


def exchange(process, message, transcript):
    raw = json.dumps(message)
    transcript.append(">> " + raw)
    process.stdin.write(raw + "\n")
    process.stdin.flush()
    if "id" not in message:
        return None
    line = process.stdout.readline()
    transcript.append("<< " + line.rstrip("\n"))
    if not line:
        raise RuntimeError("MCP server closed stdout")
    return json.loads(line)


def tool(process, transcript, request_id, name, arguments, *, quiet=False):
    sink = [] if quiet else transcript
    response = exchange(process, {
        "jsonrpc": "2.0", "id": request_id, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }, sink)
    result = response.get("result") or {}
    if result.get("isError"):
        transcript.extend(sink)
        raise RuntimeError(json.dumps(result))
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


def main():
    binary = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "engine/target/release/deoos-server")
    engine = Engine(binary, "rustfs")
    engine.start()
    transcript = [f"engine {engine.url}"]
    work = pathlib.Path(tempfile.mkdtemp(prefix="deoos-mcp-drive-"))
    marker = work / "marker.txt"
    task_id = "mcp-survive"
    workflow = work / "workflow.py"
    subprocess.run(
        [sys.executable, str(ROOT / "agents/scripts/scaffold_workflow.py"), str(work)],
        check=True,
    )
    env = os.environ.copy()
    env.update(
        ENGINE_URL=engine.url,
        PYTHONPATH=str(ROOT / "clients" / "python"),
        DEOOS_MODE="server",
        DEOOS_TASK_ID=task_id,
        DEOOS_MARKER=str(marker),
        DEOOS_HOLD_SECONDS="30",
    )
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "DEOOS_FEEDBACK", "ENGINE_TOKEN"):
        env.pop(name, None)
    stderr_path = work / "mcp.stderr"
    stderr_file = stderr_path.open("w", encoding="utf-8")
    mcp = subprocess.Popen(
        [sys.executable, "-m", "deoos", "mcp", "--allow-actions"],
        env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file,
        text=True, encoding="utf-8",
    )
    worker = None
    try:
        ready = exchange(mcp, {
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "deoos-proof", "version": "1"},
            },
        }, transcript)
        if ready.get("result", {}).get("protocolVersion") != "2025-11-25":
            raise RuntimeError(json.dumps(ready))
        exchange(mcp, {"jsonrpc": "2.0", "method": "notifications/initialized"}, transcript)
        health = tool(mcp, transcript, 2, "engine_health", {})
        if health.get("protocol_version") != 3 or not health.get("ok"):
            raise RuntimeError(json.dumps(health))
        docs = tool(mcp, transcript, 3, "docs_search", {"query": "wait_signal"})
        if not any("wait_signal" in item.get("text", "") for item in docs.get("snippets", [])):
            raise RuntimeError("docs_search missed wait_signal")
        started = tool(mcp, transcript, 4, "task_submit", {
            "id": task_id, "handler": "survive", "inputs": {"marker": str(marker)},
        })
        if started.get("status") != "queued":
            raise RuntimeError(json.dumps(started))
        worker = subprocess.Popen(
            [sys.executable, str(workflow)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 40
        summary = None
        call_id = 5
        while time.monotonic() < deadline:
            call_id += 1
            summary = tool(mcp, transcript, call_id, "task_summary", {"id": task_id}, quiet=True)
            steps = set(summary.get("completed_steps") or [])
            text = marker.read_text(encoding="utf-8") if marker.exists() else ""
            if "work" in steps and "hold" not in steps and text.count("hold\n") >= 1:
                break
            time.sleep(0.3)
        else:
            detail = marker.read_text(encoding="utf-8") if marker.exists() else "no marker"
            raise RuntimeError(f"hold did not start: {summary}\n{detail}")
        transcript.append("<< task_summary during hold " + json.dumps(summary, sort_keys=True))
        os.kill(worker.pid, signal.SIGKILL)
        worker.wait(timeout=10)
        worker = None
        history = tool(mcp, transcript, 80, "task_history", {"id": task_id})
        transcript.append(
            f"after kill: status={history.get('status')} "
            f"events={[event.get('event') for event in history.get('history', [])]}"
        )
        worker = subprocess.Popen(
            [sys.executable, str(workflow)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 40
        final = None
        while time.monotonic() < deadline:
            call_id += 1
            final = tool(mcp, transcript, call_id, "task_summary", {"id": task_id}, quiet=True)
            if final.get("status") in ("completed", "failed", "cancelled"):
                break
            time.sleep(0.3)
        else:
            raise RuntimeError("task did not finish: " + json.dumps(final))
        transcript.append("<< task_summary after restart " + json.dumps(final, sort_keys=True))
        if final.get("status") != "completed" or set(final.get("completed_steps") or []) < {"work", "hold"}:
            raise RuntimeError(json.dumps(final))
        text = marker.read_text(encoding="utf-8")
        if text.count("work\n") != 1 or text.count("hold\n") != 2:
            raise RuntimeError("marker:\n" + text)
        listed = tool(mcp, transcript, 200, "task_list", {})
        if task_id not in {item["id"] for item in listed.get("tasks", [])}:
            raise RuntimeError("task missing from list")
        feedback = tool(mcp, transcript, 201, "feedback", {
            "tried": "mcp task_submit survive, kill worker during hold, start worker again",
            "happened": "task_summary reached completed with steps work and hold",
            "dry_run": True,
        })
        if feedback.get("dry_run") is not True or feedback.get("labels") != ["agent-feedback"]:
            raise RuntimeError(json.dumps(feedback))
        if "AKIA" in json.dumps(feedback):
            raise RuntimeError("feedback echoed a sample key")
        transcript.append("marker:\n" + text.rstrip("\n"))
        transcript.append("PASS")
        sys.stdout.write("\n".join(transcript) + "\n")
        return 0
    finally:
        if worker is not None and worker.poll() is None:
            os.kill(worker.pid, signal.SIGKILL)
            worker.wait(timeout=10)
        if mcp.poll() is None:
            mcp.stdin.close()
            mcp.wait(timeout=10)
        stderr_file.close()
        engine.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"FAIL {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1)
