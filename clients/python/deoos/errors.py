"""Stable, actionable error text shared by the CLI and the Python SDK."""

DOCS = "https://deoos.dev/errors#"


def guide(code, what, cause, fix):
    return f"[{code}] {what} Cause: {cause} Fix: {fix} {DOCS}{code}"


def engine_unreachable(url):
    return guide(
        "ENGINE_UNREACHABLE",
        f"cannot reach engine at {url}",
        "nothing accepted a connection. deoos-server is not running, or ENGINE_URL is the wrong host or port",
        "from a checkout run `make dev`, then `export ENGINE_URL=http://127.0.0.1:7331` and "
        '`export PYTHONPATH="$PWD/clients/python"`. `python -m deoos info` should print "protocol_version": 3',
    )


def engine_wrong_url(url):
    return guide(
        "ENGINE_WRONG_URL",
        f"ENGINE_URL {url} responded but is not a deoos server",
        "the URL points at a different service, such as RustFS on port 19000, or the response is not deoos JSON",
        "set ENGINE_URL to the deoos-server origin only, usually http://127.0.0.1:7331 from `make dev`, "
        "with no path suffix. The server log prints `deoos-server listening`",
    )


def protocol_mismatch(reported):
    got = "missing" if reported is None else str(reported)
    return guide(
        "PROTOCOL_MISMATCH",
        f"engine protocol version 3 is required; server reported {got}",
        "this SDK and deoos-server are different versions",
        "install the Python wheel and the Node package from the same GitHub release archive as "
        "`deoos-server --version`. Do not install deoos from PyPI or the npm registry. "
        "Upgrade so this SDK and the server match",
    )


def bucket_required():
    return guide(
        "BUCKET_REQUIRED",
        "bucket or DEOOS_STORAGE_BUCKET is required for library mode",
        "library mode talks to object storage in-process and does not use ENGINE_URL",
        "set DEOOS_STORAGE_BUCKET, or set DEOOS_MODE=server and ENGINE_URL=http://127.0.0.1:7331 after `make dev`",
    )


def engine_url_required():
    return guide(
        "ENGINE_URL_REQUIRED",
        "DEOOS_MODE=server requires ENGINE_URL",
        "server mode has no storage credentials and needs the running deoos-server",
        "start a server with `make dev` and export ENGINE_URL=http://127.0.0.1:7331",
    )


def mode_invalid():
    return guide(
        "MODE_INVALID",
        "DEOOS_MODE must be 'library' or 'server'",
        "the value is not one of the two modes",
        "export DEOOS_MODE=server with ENGINE_URL, or DEOOS_MODE=library with DEOOS_STORAGE_BUCKET. "
        "Unset DEOOS_MODE to follow ENGINE_URL",
    )


def invalid_name():
    return guide(
        "INVALID_NAME",
        "invalid name; use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
        "task ids, handlers, steps, signals, and operation ids share this rule",
        "rename it to 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    )


def native_library():
    return guide(
        "NATIVE_LIBRARY",
        "Cannot load the DEOOS engine in library mode",
        "the prebuilt library is missing, or it was built for another operating system or CPU",
        "install the Python wheel from the GitHub release archive for this OS and CPU. "
        "If DEOOS_NATIVE_LIBRARY is set, point it at that library. "
        "To use a running server instead, call Client.remote(url, token). Do not install deoos from PyPI",
    )


def node_library():
    return guide(
        "NODE_LIBRARY",
        "Cannot load the DEOOS engine in library mode",
        "the prebuilt Node addon is missing, or it was built for another operating system, CPU, or Node version",
        "install the Node package from the GitHub release archive for this OS and CPU, using Node 22 or newer. "
        "If DEOOS_NODE_LIBRARY is set, point it at deoos_node.node. "
        "To use a running server instead, call Client.remote(url, token). Do not install deoos from the npm registry",
    )
