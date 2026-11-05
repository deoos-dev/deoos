# Document index

Indexing a folder should keep the documents already read when the worker dies, including a crash after the index is published and before that checkpoint commits. Search is lowercase ASCII words. Original text, including a BOM and CRLF, is kept.

## Run

```sh
docker compose run --build --rm document-index
```

## Expected output

```text
library-python: killed twice; python → typescript → python; 8 documents extracted once, publication replayed, 10 searches verified.
```

Python and TypeScript search agree. The same check runs against `deoos-server`. Files stay in `outputs/document-index/`.
