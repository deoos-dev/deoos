"""File a deoos problem as a GitHub issue, or return a prefilled issue URL.

The report contains the deoos version, what was tried, what happened, and an
optional repro. It does not read the workflow, the working tree, or environment
variables. Secret-like text is refused unless the caller sets include_secrets.
"""
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

from . import __version__

REPO = "deoos-dev/deoos"
MARKER = "<!-- deoos-agent-feedback -->"
_SECRET = re.compile(
    r"(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{20,}|github_pat_|"
    r"BEGIN (?:RSA |OPENSSH )?PRIVATE KEY|xox[baprs]-|AWS_SECRET_ACCESS_KEY\s*=)",
    re.IGNORECASE,
)
_SECRET_ENV = (
    "ENGINE_TOKEN",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
)


def deoos_version():
    try:
        from importlib.metadata import version
        return version("deoos")
    except Exception:
        return __version__


def file_feedback(tried, happened, repro=None, include_secrets=False):
    tried = _text("tried", tried, required=True)
    happened = _text("happened", happened, required=True)
    repro = _text("repro", repro, required=False)
    if not isinstance(include_secrets, bool):
        raise ValueError("include_secrets must be a boolean")
    body_text = "\n".join(part for part in (tried, happened, repro) if part)
    if not include_secrets and _looks_like_secret(body_text):
        raise ValueError(
            "feedback looks like it contains a secret or credential; remove it, "
            "or pass include_secrets if you intend to send it. Nothing was filed."
        )
    version = deoos_version()
    title = _title(happened)
    body = _body(tried, happened, repro, version)
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    fallback = _prefilled_url(title, body)
    if not token:
        return {
            "filed": False,
            "version": version,
            "url": fallback,
            "note": "No GITHUB_TOKEN or GH_TOKEN; open the url to file the issue. "
                    "The title prefix lets the triage workflow add the agent-feedback label.",
        }
    try:
        url = _create_issue(token, title, body)
    except Exception as error:
        return {
            "filed": False,
            "version": version,
            "url": fallback,
            "note": f"GitHub API failed ({type(error).__name__}); open the url to file the issue.",
        }
    return {"filed": True, "version": version, "url": url, "note": "Filed with the agent-feedback label."}


def _text(label, value, *, required):
    if value is None or value == "":
        if required:
            raise ValueError(f"{label} must be a non-empty string")
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    if len(value) > 4000:
        raise ValueError(f"{label} must be at most 4000 characters")
    if required and not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _looks_like_secret(text):
    if _SECRET.search(text):
        return True
    for name in _SECRET_ENV:
        value = os.environ.get(name)
        if value and len(value) >= 8 and value in text:
            return True
    return False


def _title(happened):
    line = " ".join(happened.split())
    if len(line) > 80:
        line = line[:79].rstrip() + "..."
    return f"[agent-feedback] {line}"


def _engine_line():
    try:
        from . import client_from_environment
        client = client_from_environment()
    except Exception:
        return "engine: not configured"
    try:
        try:
            info = client.request("/info")
        except Exception:
            return "engine: unreachable"
        protocol = info.get("protocol_version") if isinstance(info, dict) else None
        if protocol is None:
            return "engine: reachable"
        return f"engine protocol: {protocol}"
    finally:
        client.close()


def _body(tried, happened, repro, version):
    repro_text = repro if repro else (
        "_Not included. Pass repro only when it has no secrets and you mean to share it._"
    )
    return "\n".join([
        MARKER,
        "",
        "## What I tried",
        tried,
        "",
        "## What happened",
        happened,
        "",
        "## Repro",
        repro_text,
        "",
        "## Environment",
        f"- deoos: {version}",
        f"- {_engine_line()}",
        "",
        "Sent by `python -m deoos feedback` or the MCP `feedback` tool. "
        "The report is only the text above, the deoos version, and whether the engine responded.",
    ])


def _prefilled_url(title, body):
    query = urllib.parse.urlencode(
        {"title": title, "body": body[:6000], "labels": "agent-feedback"},
        quote_via=urllib.parse.quote,
    )
    return f"https://github.com/{REPO}/issues/new?{query}"


def _github_json(method, url, token, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "User-Agent": "deoos-feedback",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"message": raw[:500]}
        return error.code, parsed


def _create_issue(token, title, body):
    label_status, _ = _github_json(
        "POST", f"https://api.github.com/repos/{REPO}/labels", token,
        {"name": "agent-feedback", "color": "5319e7",
         "description": "Filed by deoos feedback"},
    )
    if label_status not in (201, 422):
        raise RuntimeError(f"GitHub label request returned {label_status}")
    status, payload = _github_json(
        "POST", f"https://api.github.com/repos/{REPO}/issues", token,
        {"title": title, "body": body, "labels": ["agent-feedback"]},
    )
    if status != 201 or not isinstance(payload, dict) or not payload.get("html_url"):
        raise RuntimeError(f"GitHub issue request returned {status}")
    return payload["html_url"]
