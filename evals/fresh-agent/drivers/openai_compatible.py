"""Ask an OpenAI-compatible chat model to write app/workflow.py.

The working directory is the eval sandbox. The model sees only the files in
that directory. Set OPENAI_API_KEY. OPENAI_BASE_URL defaults to
https://api.openai.com/v1. OPENAI_MODEL defaults to gpt-4.1-mini.

This driver does not score the result. The harness runs the file it writes
against a real deoos server.
"""
import json
import os
import pathlib
import sys
import urllib.request

ROOT_FILES = (
    "PROMPT.md",
    "llms.txt",
    "README.md",
    "agents/SKILL.md",
    "docs/agent-feedback.md",
    "agents/scripts/check_engine.py",
    "agents/scripts/debug_run.py",
    "agents/scripts/scaffold_workflow.py",
)


def main():
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        print("OPENAI_API_KEY is required. Nothing was sent.", file=sys.stderr)
        return 2
    workspace = pathlib.Path.cwd()
    parts = []
    for relative in ROOT_FILES:
        path = workspace / relative
        if path.is_file():
            parts.append(f"----- {relative} -----\n{path.read_text(encoding='utf-8')}")
    prompt = (
        "You are a coding agent with no other context. Reply with only the "
        "contents of app/workflow.py. No markdown fence.\n\n" + "\n\n".join(parts)
    )
    base = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
    model = os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini"
    payload = json.dumps({
        "model": model,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    request = urllib.request.Request(
        base + "/chat/completions",
        data=payload,
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        body = json.load(response)
    text = body["choices"][0]["message"]["content"]
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
    target = workspace / "app" / "workflow.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text.strip() + "\n", encoding="utf-8")
    print(target)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1)
