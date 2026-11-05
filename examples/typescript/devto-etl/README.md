# DEV articles to CSV

Same CSV export as the Python example. The Docker command runs both languages.

## Run

```sh
docker compose run --build --rm devto-etl
```

## Expected output

```text
server-typescript: recovered from both kills; 90 rows, each page fetched once, CSV safely replaced.
```

The row count is the number of articles in the three captured pages.
