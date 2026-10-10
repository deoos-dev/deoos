#!/bin/sh
set -eu

demo=${1:?Choose hacker-news, devto-etl or payment-resume.}
shift
case "$demo" in
  hacker-news)
    exec python /app/examples/hacker_news_demo.py \
      --endpoint http://rustfs:9000 --directory /app/outputs/hacker-news "$@"
    ;;
  devto-etl|payment-resume)
    script=$(printf '%s' "$demo" | tr '-' '_')
    exec python "/app/examples/${script}_demo.py" \
      --endpoint http://rustfs:9000 --directory "/app/outputs/$demo" \
      --server /opt/deoos/bin/deoos-server \
      --node-sdk /app/node_modules/deoos/dist/index.js "$@"
    ;;
  *) echo "Unknown demo: $demo" >&2; exit 1 ;;
esac
