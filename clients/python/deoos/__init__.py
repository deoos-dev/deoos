"""Worker SDK: library mode or an explicit server-mode connection."""
__version__ = "0.8.0"
import hashlib
import http.client
import json
import math
import random
import ssl
import threading
import time
import urllib.request
import urllib.error
import re
import uuid
from collections.abc import Mapping

from .errors import (
    bucket_required, engine_unreachable, engine_url_required, engine_wrong_url,
    guide, invalid_name, mode_invalid, native_library, protocol_mismatch,
)

_CODE = re.compile(r"\[([A-Z][A-Z0-9_]+)\]")

class EngineError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        self.message = message
        found = _CODE.search(message or "")
        self.code = found.group(1) if found else None
        super().__init__(f"{status}: {message}")

class ChildFailed(RuntimeError):
    def __init__(self, task_id, status, reason=None):
        self.task_id = task_id
        self.status = status
        self.reason = reason
        super().__init__(f"Child {task_id} {status}: {reason or 'no error detail'}")

class _Suspended(BaseException):
    pass

def _foreign_body(body):
    sample = (body or "").lstrip()[:800].lower()
    return sample.startswith("<") or "<html" in sample or "<error>" in sample

def _valid_step(name):
    if (not isinstance(name, str) or not name or len(name) > 128
            or name in (".", "..")
            or not name.isascii()
            or any(not (char.isalnum() or char in "_.-") for char in name)):
        raise ValueError(invalid_name())

def _safe_integer(value, name, *, minimum=0):
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < minimum or value > 9_007_199_254_740_991):
        raise ValueError(guide(
            "INVALID_INTEGER",
            f"{name} must be a safe integer >= {minimum}",
            f"{name} was missing, a boolean, a float, or outside the safe integer range",
            f"pass an integer >= {minimum} and <= 9007199254740991",
        ))

class Client:
    """Library mode by default; remote() explicitly selects server mode."""
    def __init__(self, url=None, *, bucket=None, prefix=None, token=None, **config):
        self.url = url.rstrip("/") if url else None
        self.token = token
        self.native = None
        if self.url is None:
            from .native import NativeEngine
            import os
            provider = config.get("provider") or os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
            config["provider"] = provider
            if provider == "filesystem":
                if bucket is not None:
                    raise ValueError(guide(
                        "CONFIG_FILESYSTEM",
                        "filesystem storage uses directory, not bucket",
                        "provider=filesystem does not take a bucket",
                        "pass directory or set DEOOS_STORAGE_DIRECTORY, or set DEOOS_STORAGE_PROVIDER=s3 and DEOOS_STORAGE_BUCKET",
                    ))
                directory = config.get("directory") or os.environ.get("DEOOS_STORAGE_DIRECTORY")
                if not directory:
                    raise ValueError(guide(
                        "CONFIG_DIRECTORY",
                        "filesystem storage requires directory or DEOOS_STORAGE_DIRECTORY",
                        "provider=filesystem stores task state on a local disk",
                        "set DEOOS_STORAGE_DIRECTORY to a private writable directory",
                    ))
                config["directory"] = os.fspath(directory)
            else:
                config["bucket"] = bucket or os.environ.get("DEOOS_STORAGE_BUCKET") or (
                    os.environ.get("AWS_BUCKET") if provider == "s3" else None)
                if not config["bucket"]:
                    raise ValueError(bucket_required())
            config["prefix"] = prefix or os.environ.get("EXECUTION_PREFIX", "deoos")
            if "lease_ms" not in config and "LEASE_MS" in os.environ:
                config["lease_ms"] = int(os.environ["LEASE_MS"])
            self.native = NativeEngine(config)

    @classmethod
    def remote(cls, url, token=None):
        if not url:
            raise ValueError(guide(
                "ENGINE_URL_REQUIRED",
                "server URL is required",
                "Client.remote was called without a URL",
                "pass the deoos-server origin, usually http://127.0.0.1:7331 from `make dev`",
            ))
        return cls(url, token=token)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.native is not None:
            self.native.close()

    def request(self, path, data=None):
        if data is not None:
            try:
                info = self.request("/info")
            except EngineError as error:
                if error.status == 404 and not error.code:
                    raise EngineError(409, protocol_mismatch(None)) from error
                raise
            reported = info.get("protocol_version") if isinstance(info, dict) else None
            if not isinstance(info, dict) or reported != 3:
                raise EngineError(409, protocol_mismatch(reported))
            if not isinstance(data, dict):
                raise TypeError("mutation data must be an object")
            data = {**data, "protocol_version": 3}
        if self.native is not None:
            return self.native.request("GET" if data is None else "POST", path, data)
        body = None if data is None else json.dumps(data, allow_nan=False).encode()
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(self.url + path, data=body, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                raw = response.read()
            try:
                return json.loads(raw.decode())
            except json.JSONDecodeError as error:
                raise EngineError(502, engine_wrong_url(self.url)) from error
        except urllib.error.HTTPError as error:
            detail = error.read().decode(errors="replace")
            if _foreign_body(detail):
                raise EngineError(error.code, engine_wrong_url(self.url)) from error
            raise EngineError(error.code, detail or error.reason) from error
        except urllib.error.URLError as error:
            raise OSError(engine_unreachable(self.url)) from error

    def submit(self, task_id, handler, inputs, max_attempts=3, retry_ms=0):
        return self.request("/tasks", dict(id=task_id, handler=handler, inputs=inputs,
                                         max_attempts=max_attempts, retry_ms=retry_ms))

    def inspect(self, task_id):
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}")

    def view(self, task_id):
        """Readable task state including inputs/output, without internal coordination fields."""
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}/view")

    def history(self, task_id):
        """The last 32 recorded events, separately from the task view."""
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}/history")["history"]

    def summary(self, task_id):
        """Return persisted progress metadata without inputs, outputs, or checkpoint values."""
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}/summary")

    def list_tasks(self):
        return self.request("/tasks")

    def retry(self, task_id, expected_revision, operation_id=None):
        _valid_step(task_id)
        _valid_step(expected_revision)
        if operation_id is None:
            operation_id = str(uuid.uuid4())
        _valid_step(operation_id)
        return self.request(f"/tasks/{task_id}/retry", dict(
            operation_id=operation_id, expected_revision=expected_revision,
        ))

    def schedule(self, schedule_id, handler, inputs, interval_ms, first_due_ms=None,
                 missed="latest", overlap="skip", max_attempts=3, retry_ms=0):
        """Create or inspect an immutable recurring schedule definition."""
        _valid_step(schedule_id)
        _valid_step(handler)
        _safe_integer(interval_ms, "interval_ms", minimum=1)
        if first_due_ms is not None:
            _safe_integer(first_due_ms, "first_due_ms")
        if missed not in ("latest", "catchup"):
            raise ValueError("missed must be 'latest' or 'catchup'")
        if overlap not in ("allow", "skip"):
            raise ValueError("overlap must be 'allow' or 'skip'")
        _safe_integer(max_attempts, "max_attempts", minimum=1)
        _safe_integer(retry_ms, "retry_ms")
        return self.request("/schedules", dict(
            id=schedule_id, handler=handler, inputs=inputs, interval_ms=interval_ms,
            first_due_ms=first_due_ms, missed=missed, overlap=overlap,
            max_attempts=max_attempts, retry_ms=retry_ms,
        ))

    def inspect_schedule(self, schedule_id):
        _valid_step(schedule_id)
        return self.request(f"/schedules/{schedule_id}")

    def pause_schedule(self, schedule_id):
        _valid_step(schedule_id)
        return self.request(f"/schedules/{schedule_id}/pause", {})

    def resume_schedule(self, schedule_id):
        _valid_step(schedule_id)
        return self.request(f"/schedules/{schedule_id}/resume", {})

    def backfill(self, schedule_id, start_ms, end_ms, limit=100):
        _valid_step(schedule_id)
        _safe_integer(start_ms, "start_ms")
        _safe_integer(end_ms, "end_ms")
        _safe_integer(limit, "limit", minimum=1)
        if end_ms <= start_ms:
            raise ValueError("end_ms must be greater than start_ms")
        return self.request(f"/schedules/{schedule_id}/backfill", dict(
            start_ms=start_ms, end_ms=end_ms, limit=limit,
        ))

    def signal(self, task_id, name, value, operation_id=None):
        _valid_step(task_id)
        _valid_step(name)
        if operation_id is None:
            operation_id = str(uuid.uuid4())
        _valid_step(operation_id)
        return self.request(f"/tasks/{task_id}/signals/{name}",
                            {"operation_id": operation_id, "value": value})

    def cancel(self, task_id):
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}/cancel", {})

    def run_once(self, handlers, worker_id=None):
        return self._run_once(handlers, worker_id)

    def run_worker(self, handlers, *, stop_event, poll_interval=0.1,
                   worker_id=None, on_error=None):
        """Run one polling loop until an application-owned Event is set.

        Shutdown interrupts idle waiting and finishes an in-flight run_once.
        Errors propagate by default. on_error(error, task_id) must return
        'continue' or 'propagate'; task_id is set only after the task's failure
        was successfully recorded. This does not classify the error's origin:
        an error inside a handler may come from application or storage code.
        Claim, ownership, and terminal mutation failures have no task_id.
        'continue' waits poll_interval after a recorded task failure. Failures
        without a task_id use jittered backoff capped at five seconds, reset
        after a successful poll. Shutdown interrupts either wait.
        KeyboardInterrupt and SystemExit always propagate. No signals or
        client lifecycle are managed here.
        """
        if not isinstance(handlers, Mapping) or not handlers:
            raise ValueError("handlers must be a nonempty mapping of callables")
        handlers = dict(handlers)
        for name, handler in handlers.items():
            _valid_step(name)
            if not callable(handler):
                raise ValueError("handlers must be a nonempty mapping of callables")
        if not isinstance(stop_event, threading.Event):
            raise ValueError("stop_event must be an application-owned threading.Event")
        if (isinstance(poll_interval, bool) or not isinstance(poll_interval, (int, float))
                or poll_interval <= 0 or poll_interval > threading.TIMEOUT_MAX
                or not math.isfinite(poll_interval)):
            raise ValueError("poll_interval must be a positive finite number of seconds")
        if on_error is not None and not callable(on_error):
            raise ValueError("on_error must be callable")
        worker_id = str(uuid.uuid4()) if worker_id is None else worker_id
        _valid_step(worker_id)
        backoff_base = min(poll_interval, 5.0)
        backoff = backoff_base
        while not stop_event.is_set():
            failed_task_id = None

            def recorded_failure(task_id):
                nonlocal failed_task_id
                failed_task_id = task_id

            try:
                worked = self._run_once(handlers, worker_id, recorded_failure)
            except Exception as error:
                if on_error is None:
                    raise
                decision = on_error(error, failed_task_id)
                if decision == "propagate":
                    raise
                if decision != "continue":
                    raise ValueError("on_error must return 'continue' or 'propagate'") from error
                if failed_task_id is None:
                    backoff = min(5.0, backoff * 2)
                    stop_event.wait(random.uniform(backoff_base, backoff))
                else:
                    stop_event.wait(poll_interval)
            else:
                backoff = backoff_base
                if not worked:
                    stop_event.wait(poll_interval)

    def _run_once(self, handlers, worker_id=None, recorded_failure=None):
        task = self.request("/claim", dict(worker=worker_id or str(uuid.uuid4()), handlers=list(handlers)))["task"]
        if task is None:
            return False
        if task.get("version") != 3:
            raise RuntimeError(protocol_mismatch(task.get("version")))
        ctx = Context(self, task)
        stop = threading.Event()
        def heartbeat():
            while not stop.wait(ctx.heartbeat_seconds):
                try:
                    ctx.mutate("renew")
                except Exception as error:
                    ctx.ownership_error = error
                    return
        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            try:
                output = handlers[task["handler"]](ctx, task["inputs"])
                ctx.check_owner()
            except _Suspended:
                return True
            except BaseException as error:
                if ctx.suspended:
                    return True
                if isinstance(error, Exception) and ctx.ownership_error is None:
                    failure = {"error": str(error), "terminal": True} if isinstance(error, ChildFailed) else str(error)
                    ctx.mutate("fail", failure)
                    if recorded_failure is not None:
                        recorded_failure(task["id"])
                raise
            else:
                ctx.mutate("complete", output)
            return True
        finally:
            stop.set(); thread.join()

class Context:
    def __init__(self, client, task):
        self.client, self.task = client, task
        self.ownership_error = None
        self.suspended = False
        self.owned_until = task["expires_at"]

    @property
    def heartbeat_seconds(self):
        return max(0.1, (self.owned_until - time.time()*1000) / 3000)

    def check_owner(self):
        if self.suspended:
            raise _Suspended()
        if self.ownership_error is not None:
            raise RuntimeError(guide(
                "OWNERSHIP_LOST",
                "ownership renewal failed; stop work",
                "the lease renewal did not succeed, so this attempt must not commit",
                "stop this attempt and let a worker claim the task again. Raise LEASE_MS (minimum 1000) if renewals cannot keep up",
            )) from self.ownership_error

    def current_state(self):
        self.check_owner()
        state = self.client.request(f'/tasks/{self.task["id"]}')
        if state.get("status") != "running" or state.get("token") != self.task["token"]:
            error = EngineError(
                409,
                guide(
                    "OWNERSHIP_LOST",
                    "ownership lost or task is no longer running; stop this attempt and let a worker claim it again",
                    "the lease ended or another worker claimed the task",
                    "stop this attempt. A worker can claim the task again after the lease",
                ),
            )
            self.ownership_error = error
            raise error
        return state

    def idempotency_key(self, step):
        return f'{self.task["id"]}/{step}'

    def mutate(self, action, value=None):
        # Snapshot once: retries must keep both the operation ID and body unchanged.
        data = json.loads(json.dumps(dict(token=self.task["token"],
                          operation_id=str(uuid.uuid4()), value=value), allow_nan=False))
        # Bound retry starts; never cancel an in-flight write and its reconciliation.
        deadline = time.monotonic() + min(10, max(0, (self.owned_until - time.time()*1000) / 2000))
        for attempt in range(3):
            try:
                state = self.client.request(f'/tasks/{self.task["id"]}/{action}', data)
                if action == "renew":
                    self.owned_until = max(self.owned_until, state["expires_at"])
                return state
            except (EngineError, urllib.error.URLError, TimeoutError, ConnectionError,
                    http.client.IncompleteRead, ssl.SSLError) as error:
                transient = not isinstance(error, EngineError) or error.status == 503
                delay = random.uniform(.1, .5)
                retryable = action in ("renew", "suspend", "complete", "fail") or action.startswith(("definitions/", "steps/"))
                if (not transient or not retryable or attempt == 2
                        or self.ownership_error is not None and action not in ("suspend", "complete", "fail")
                        or time.monotonic() + delay >= deadline
                        or time.time()*1000 + delay*1000 >= self.owned_until):
                    raise
                time.sleep(delay)
                if (time.monotonic() >= deadline or time.time()*1000 >= self.owned_until
                        or self.ownership_error is not None and action not in ("suspend", "complete", "fail")):
                    raise

    def log(self, message):
        if not isinstance(message, str) or len(message.encode("utf-8")) > 4096:
            raise ValueError("message must be a string of at most 4096 UTF-8 bytes")
        self.current_state()
        return self.mutate("log", message)

    def step(self, name, function, revision="1"):
        return self._checkpoint(name, {"kind": "step", "revision": revision}, function)

    def _checkpoint(self, name, definition, function):
        _valid_step(name)
        _valid_step(definition["revision"])
        self.check_owner()
        try:
            state = self.mutate("definitions/" + name, definition)
        except EngineError as error:
            if error.status == 409:
                self.current_state()
            raise
        if name in state["steps"]:
            return self.client.request(f'/tasks/{self.task["id"]}/steps/{name}')
        result = function()
        self.check_owner()
        self.mutate("steps/" + name, result)
        return result

    def spawn(self, name, handler, inputs, max_attempts=3, retry_ms=0):
        _valid_step(name)
        child_id = "child-" + hashlib.sha256(f'{self.task["id"]}/{name}'.encode()).hexdigest()[:32]

        def submit_child():
            self.client.submit(child_id, handler, inputs, max_attempts, retry_ms)
            return child_id

        definition = {
            "kind": "spawn", "revision": "1", "child_id": child_id,
            "handler": handler, "inputs": inputs,
            "max_attempts": max_attempts, "retry_ms": retry_ms,
        }
        return self._checkpoint(name, definition, submit_child)

    def join(self, name, children):
        _valid_step(name)
        if isinstance(children, str):
            raise ValueError("children must be a sequence of task IDs")
        children = list(children)

        def collect():
            states = [self.client.request(f"/tasks/{child_id}") for child_id in children]
            if any(state["status"] not in ("completed", "failed", "cancelled") for state in states):
                self.mutate("suspend", {"children": children})
                self.suspended = True
                raise _Suspended()
            failed = next((state for state in states if state["status"] != "completed"), None)
            if failed is not None:
                raise ChildFailed(failed["id"], failed["status"], failed.get("error"))
            return [state.get("output") for state in states]

        return self._checkpoint(name, {"kind": "join", "revision": "1", "children": children}, collect)

    def sleep(self, name, milliseconds):
        _valid_step(name)
        if (isinstance(milliseconds, bool) or not isinstance(milliseconds, int)
                or milliseconds < 0 or milliseconds > 9_007_199_254_740_991):
            raise ValueError("milliseconds must be a nonnegative safe integer")

        def wait():
            task = self.mutate("suspend", {"timer": {"name": name, "milliseconds": milliseconds}})
            if task["status"] == "waiting":
                self.suspended = True
                raise _Suspended()
            if task["status"] != "running":
                raise RuntimeError(f"unexpected timer state: {task['status']}")
            return None

        return self._checkpoint(
            name, {"kind": "sleep", "revision": "1", "milliseconds": milliseconds}, wait
        )

    def wait_signal(self, name):
        _valid_step(name)

        def wait():
            task = self.mutate("suspend", {"signal": name})
            if task["status"] == "waiting":
                self.suspended = True
                raise _Suspended()
            if task["status"] != "running":
                raise RuntimeError(f"unexpected signal state: {task['status']}")
            return self.client.request(f"/tasks/{self.task['id']}/signals/{name}")

        return self._checkpoint(name, {"kind": "wait_signal", "revision": "1", "signal": name}, wait)


def client_from_environment():
    """Open the process-local client used by the CLI and MCP server.

    ``DEOOS_MODE=library`` uses in-process storage and ignores ``ENGINE_URL``.
    ``DEOOS_MODE=server`` requires ``ENGINE_URL``. When ``DEOOS_MODE`` is unset,
    ``ENGINE_URL`` selects server mode and its absence selects library mode.
    """
    import os
    import sys
    mode = os.environ.get("DEOOS_MODE")
    url = os.environ.get("ENGINE_URL")
    token = os.environ.get("ENGINE_TOKEN")
    if mode == "library":
        if url:
            print("DEOOS_MODE=library ignores ENGINE_URL. "
                  "Unset ENGINE_URL, or set DEOOS_MODE=server.", file=sys.stderr)
        return Client()
    if mode == "server":
        if not url:
            raise ValueError(engine_url_required())
        return Client.remote(url, token)
    if mode:
        raise ValueError(mode_invalid())
    if url:
        return Client.remote(url, token)
    return Client(token=token)
