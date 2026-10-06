"""Worker SDK: in-process library or explicit shared-server connection."""
import hashlib
import json
import math
import threading
import time
import urllib.request
import urllib.error
import uuid
from collections.abc import Mapping

class EngineError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(f"{status}: {message}")

class ChildFailed(RuntimeError):
    def __init__(self, task_id, status, reason=None):
        self.task_id = task_id
        self.status = status
        self.reason = reason
        super().__init__(f"Child {task_id} {status}: {reason or 'no error detail'}")

class _Suspended(BaseException):
    pass

def _valid_step(name):
    if (not isinstance(name, str) or not name or len(name) > 128
            or name in (".", "..")
            or not name.isascii()
            or any(not (char.isalnum() or char in "_.-") for char in name)):
        raise ValueError("invalid step name")

def _safe_integer(value, name, *, minimum=0):
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < minimum or value > 9_007_199_254_740_991):
        raise ValueError(f"{name} must be a safe integer >= {minimum}")

class Client:
    """In-process library by default; remote() explicitly selects shared-server mode."""
    def __init__(self, url=None, *, bucket=None, prefix=None, token=None, **config):
        self.url = url.rstrip("/") if url else None
        self.token = token
        self.native = None
        if self.url is None:
            from .native import NativeEngine
            import os
            provider = config.get("provider") or os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
            config["bucket"] = bucket or os.environ.get("DEOOS_STORAGE_BUCKET") or (
                os.environ.get("AWS_BUCKET") if provider == "s3" else None)
            if not config["bucket"]:
                raise ValueError("bucket is required for library mode")
            config["prefix"] = prefix or os.environ.get("EXECUTION_PREFIX", "durable-v3")
            if "lease_ms" not in config and "LEASE_MS" in os.environ:
                config["lease_ms"] = int(os.environ["LEASE_MS"])
            self.native = NativeEngine(config)

    @classmethod
    def remote(cls, url, token=None):
        if not url:
            raise ValueError("server URL is required")
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
                if error.status == 404:
                    raise EngineError(409, "engine protocol version 3 is required") from error
                raise
            if not isinstance(info, dict) or info.get("protocol_version") != 3:
                raise EngineError(409, "engine protocol version 3 is required")
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
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise EngineError(error.code, error.read().decode()) from error

    def submit(self, task_id, handler, inputs, max_attempts=3, retry_ms=0):
        return self.request("/tasks", dict(id=task_id, handler=handler, inputs=inputs,
                                         max_attempts=max_attempts, retry_ms=retry_ms))

    def inspect(self, task_id):
        _valid_step(task_id)
        return self.request(f"/tasks/{task_id}")

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

    def signal(self, task_id, name, value):
        _valid_step(task_id)
        _valid_step(name)
        return self.request(f"/tasks/{task_id}/signals/{name}",
                            {"operation_id": str(uuid.uuid4()), "value": value})

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
        'continue' waits poll_interval seconds before polling.
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
                stop_event.wait(poll_interval)
            else:
                if not worked:
                    stop_event.wait(poll_interval)

    def _run_once(self, handlers, worker_id=None, recorded_failure=None):
        task = self.request("/claim", dict(worker=worker_id or str(uuid.uuid4()), handlers=list(handlers)))["task"]
        if task is None:
            return False
        if task.get("version") != 3:
            raise RuntimeError("unsupported engine protocol version")
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
            output = handlers[task["handler"]](ctx, task["inputs"])
            ctx.check_owner()
        except _Suspended:
            stop.set(); thread.join()
            return True
        except BaseException as error:
            stop.set(); thread.join()
            if ctx.suspended:
                return True
            if isinstance(error, Exception) and ctx.ownership_error is None:
                failure = {"error": str(error), "terminal": True} if isinstance(error, ChildFailed) else str(error)
                ctx.mutate("fail", failure)
                if recorded_failure is not None:
                    recorded_failure(task["id"])
            raise
        else:
            stop.set(); thread.join()
            ctx.check_owner()
            ctx.mutate("complete", output)
        return True

class Context:
    def __init__(self, client, task):
        self.client, self.task = client, task
        self.ownership_error = None
        self.suspended = False
        self.heartbeat_seconds = max(0.1, (task["expires_at"] - time.time()*1000) / 3000)

    def check_owner(self):
        if self.suspended:
            raise _Suspended()
        if self.ownership_error is not None:
            raise RuntimeError("ownership renewal failed; stop work") from self.ownership_error

    def current_state(self):
        self.check_owner()
        state = self.client.request(f'/tasks/{self.task["id"]}')
        if state.get("status") != "running" or state.get("token") != self.task["token"]:
            error = EngineError(409, "ownership lost or task is no longer running")
            self.ownership_error = error
            raise error
        return state

    def idempotency_key(self, step):
        return f'{self.task["id"]}/{step}'

    def mutate(self, action, value=None):
        data = dict(token=self.task["token"], operation_id=str(uuid.uuid4()), value=value)
        return self.client.request(f'/tasks/{self.task["id"]}/{action}', data)

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
