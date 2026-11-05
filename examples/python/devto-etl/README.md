# DEV articles to CSV

A CSV export should not fetch the same page again, or tear the file, when the worker dies. Three pages of DEV articles are normalized and the CSV is replaced only after the write succeeds.

## Run

```sh
docker compose run --build --rm devto-etl
```

The container installs deoos 0.8.0 from the checked release archive and runs the workflow against RustFS and `deoos-server`.

## Expected output

```text
Capturing three live DEV article pages…
library-python: recovered from both kills; 90 rows, each page fetched once, CSV safely replaced.
```

The row count is the number of articles in the three captured pages. Each page is fetched once per workflow. Python and TypeScript, in library mode and against `deoos-server`, write the same CSV. Files stay in `outputs/devto-etl/`.
