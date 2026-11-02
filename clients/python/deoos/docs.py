"""Search the public docs that ship with deoos.

The installed wheel carries a copy under ``deoos/bundled``. A checkout falls
back to the repository files so ``python -m deoos mcp`` works before packaging.
"""
import pathlib
import re

MAX_SNIPPETS = 5
MAX_CHARS = 800
RELATIVE = (
    "llms.txt",
    "README.md",
    "agents/SKILL.md",
    "docs/agent-feedback.md",
    "agents/scripts/check_engine.py",
    "agents/scripts/debug_run.py",
    "agents/scripts/scaffold_workflow.py",
)


def search(query):
    if not isinstance(query, str) or not query.strip() or len(query) > 200:
        raise ValueError("query must be a string of 1-200 characters")
    terms = [term for term in re.split(r"\s+", query.lower()) if len(term) > 1]
    if not terms:
        terms = [query.lower()]
    files = _files()
    if not files:
        return {"query": query, "snippets": [], "notice": "no bundled docs found"}
    ranked = []
    for source, text in files:
        for section in _sections(source, text):
            score = _score(section, terms)
            if score:
                ranked.append((score, section))
    ranked.sort(key=lambda item: (-item[0], item[1]["source"], item[1]["heading"]))
    snippets = [_snippet(section) for _, section in ranked[:MAX_SNIPPETS]]
    body = {"query": query, "snippets": snippets}
    if not snippets:
        body["notice"] = "no matching docs; try a handler, step, or command name from the skill"
    return body


def _files():
    # A checkout wins so local edits show up immediately. An installed wheel
    # has no checkout next to it and reads deoos/bundled, copied at package time.
    for parent in pathlib.Path(__file__).resolve().parents:
        if (parent / "llms.txt").is_file() and (parent / "agents" / "SKILL.md").is_file() \
                and (parent / "engine" / "Cargo.toml").is_file():
            return _read_tree(parent)
    bundled = pathlib.Path(__file__).resolve().parent / "bundled"
    if bundled.is_dir():
        found = _read_tree(bundled)
        if found:
            return found
    return []


def _read_tree(root):
    found = []
    for rel in RELATIVE:
        path = root / rel
        if path.is_file():
            found.append((rel, path.read_text(encoding="utf-8")))
    return found


def _sections(source, text):
    sections = []
    heading = source
    buffer = []
    for line in text.splitlines():
        if line.startswith("#"):
            if buffer:
                sections.append({"source": source, "heading": heading, "text": "\n".join(buffer).strip()})
            heading = line.lstrip("#").strip() or source
            buffer = [line]
        else:
            buffer.append(line)
    if buffer:
        sections.append({"source": source, "heading": heading, "text": "\n".join(buffer).strip()})
    return [section for section in sections if section["text"]]


def _score(section, terms):
    haystack = section["text"].lower()
    heading = section["heading"].lower()
    score = 0
    for term in terms:
        if term in heading:
            score += 3
        score += haystack.count(term)
    return score


def _snippet(section):
    text = section["text"]
    if len(text) > MAX_CHARS:
        cut = text.rfind("\n", 0, MAX_CHARS)
        text = text[: cut if cut > 200 else MAX_CHARS].rstrip() + "\n…"
    return {"source": section["source"], "heading": section["heading"], "text": text}
