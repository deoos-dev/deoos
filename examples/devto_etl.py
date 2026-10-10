"""Fetch DEV Community articles and write a durable CSV with standard-library tools.

Submit ``etl_inputs(str(Path("articles.csv").resolve()))`` using ``HANDLER`` and
run a DEOOS worker with ``HANDLERS``. The output path belongs to the worker host.
"""
import csv
import json
import os
from pathlib import Path
import tempfile
import urllib.parse
import urllib.request

HANDLER = "devto.etl.v1"
SOURCE = "https://dev.to/api/articles"
FIELDS = (
    "id", "title", "published_at", "url", "comments_count",
    "positive_reactions_count", "tag_list", "user.username",
)


def etl_inputs(output_path, source=SOURCE, pages=3, per_page=30):
    if (not isinstance(output_path, str) or not output_path
            or not Path(output_path).is_absolute() or not Path(output_path).name):
        raise ValueError("output_path must be an absolute local file path")
    if not isinstance(source, str):
        raise ValueError("source must be an HTTP(S) URL")
    url = urllib.parse.urlsplit(source)
    if (url.scheme not in ("http", "https") or not url.hostname or url.username
            or url.password or url.query or url.fragment):
        raise ValueError("source must be an HTTP(S) URL without credentials, query, or fragment")
    for name, value in (("pages", pages), ("per_page", per_page)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    return {"output_path": output_path, "source": source, "pages": pages, "per_page": per_page}


def fetch_page(source, page, per_page):
    query = urllib.parse.urlencode({"page": page, "per_page": per_page})
    request = urllib.request.Request(source + "?" + query, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        articles = json.load(response)
    if not isinstance(articles, list) or any(not isinstance(article, dict) for article in articles):
        raise ValueError("articles response must be an array of objects")
    return articles


def normalize(pages):
    rows, seen = [], set()
    for articles in pages:
        for article in articles:
            article_id = article.get("id")
            if isinstance(article_id, bool) or not isinstance(article_id, int) or article_id < 1:
                raise ValueError("article id must be a positive integer")
            # Pagination can overlap as new articles arrive; retain the first occurrence.
            if article_id in seen:
                continue
            seen.add(article_id)
            user = article.get("user") or {}
            if not isinstance(user, dict):
                raise ValueError("article user must be an object or null")
            row = {field: article.get(field) for field in FIELDS[:-1]}
            row["user.username"] = user.get("username")
            rows.append(row)
    return rows


def write_csv(output_path, rows):
    output = Path(output_path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="",
                                         dir=output.parent, prefix=f".{output.name}.",
                                         suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            for row in rows:
                # Nested CSV cells use JSON so Python and TypeScript write the same values.
                writer.writerow({key: csv_value(value) for key, value in row.items()})
            stream.flush()
            os.fsync(stream.fileno())
        # A replay after replacement writes the same content to the same destination.
        os.replace(temporary, output)
        return {"output_path": output_path, "articles": len(rows)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def csv_value(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return "" if value is None else str(value)


def etl(ctx, inputs):
    inputs = etl_inputs(**inputs)
    pages = []
    for page in range(1, inputs["pages"] + 1):
        pages.append(ctx.step(f"fetch-page-{page}",
                              lambda page=page: fetch_page(inputs["source"], page, inputs["per_page"])))
    rows = ctx.step("normalize", lambda: normalize(pages))
    return ctx.step("write-csv", lambda: write_csv(inputs["output_path"], rows))


HANDLERS = {HANDLER: etl}
