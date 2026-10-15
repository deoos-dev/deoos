"""Checkpoint local UTF-8 documents and atomically publish a searchable JSON index.

Submit ``index_inputs(absolute_document_paths, absolute_output_path)`` using
``HANDLER``; run a worker with ``HANDLERS``. Paths belong to the worker host.
Search uses lowercase ASCII letters/digits; original Unicode text is retained.
"""
import json
import os
from pathlib import Path
import re
import tempfile

HANDLER = "document.index.v1"


def index_inputs(documents, output_path):
    def absolute_file(value):
        return isinstance(value, str) and Path(value).is_absolute() and bool(Path(value).name)

    if not isinstance(documents, list) or not documents or not all(map(absolute_file, documents)):
        raise ValueError("documents must be a nonempty list of absolute local file paths")
    documents = [os.path.normpath(path) for path in documents]
    if len(set(documents)) != len(documents):
        raise ValueError("document paths must be unique")
    if not absolute_file(output_path):
        raise ValueError("output_path must be an absolute local file path distinct from the documents")
    output_path = os.path.normpath(output_path)
    if not absolute_file(output_path) or output_path in documents:
        raise ValueError("output_path must be an absolute local file path distinct from the documents")
    return {"documents": documents, "output_path": output_path}


def tokenize(text):
    return sorted({word.lower() for word in re.findall(r"[A-Za-z0-9]+", text)})


def publish(output_path, documents):
    terms = {}
    for document in documents:
        for token in document["tokens"]:
            terms.setdefault(token, []).append(document["id"])
    index = {"documents": documents, "terms": terms}
    output, temporary = Path(output_path), None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n",
                dir=output.parent, prefix=f".{output.name}.", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            # Sorted object keys also keep numeric tokens identical across SDKs.
            stream.write(json.dumps(index, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        return {"output_path": output_path, "documents": len(documents), "terms": len(terms)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def search(index, query):
    if not isinstance(query, str):
        raise ValueError("query must be a string")
    tokens = tokenize(query)
    if not tokens:
        return []
    matches = set(index["terms"].get(tokens[0], []))
    for token in tokens[1:]:
        matches.intersection_update(index["terms"].get(token, []))
    return [document for document in index["documents"] if document["id"] in matches]


def index_documents(ctx, inputs):
    inputs = index_inputs(**inputs)
    documents = []
    for number, path in enumerate(inputs["documents"], 1):
        # Decode bytes explicitly to retain CRLF and any UTF-8 BOM in saved text.
        text = ctx.step(f"read-{number}", lambda path=path: Path(path).read_bytes().decode("utf-8"))
        documents.append(ctx.step(f"index-{number}", lambda number=number, path=path, text=text: {
            "id": f"doc-{number}", "path": path, "text": text, "tokens": tokenize(text)}))
    return ctx.step("publish", lambda: publish(inputs["output_path"], documents))


HANDLERS = {HANDLER: index_documents}
