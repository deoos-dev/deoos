FROM node:22.23.2-bookworm-slim AS node
FROM python:3.12-slim-bookworm

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG DEOOS_VERSION=0.7.2
ARG DEOOS_RELEASE_URL=https://github.com/deoos-dev/deoos/releases/download/${DEOOS_VERSION}

# Install the same published native package for both SDKs and the server.
RUN set -eu; \
    case "$TARGETARCH" in amd64) target=x64 ;; arm64) target=arm64 ;; *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; esac; \
    archive="deoos-${DEOOS_VERSION}-linux-${target}.tar.gz"; \
    mkdir -p /tmp/release /opt/deoos /app; \
    cd /tmp/release; \
    curl -fL --retry 3 "${DEOOS_RELEASE_URL}/${archive}" -o "$archive"; \
    curl -fsSL --retry 3 "${DEOOS_RELEASE_URL}/SHA256SUMS" -o SHA256SUMS; \
    awk -v name="$archive" '$2 == name { print; found++ } END { if (found != 1) exit 1 }' SHA256SUMS > selected.sha256; \
    sha256sum -c selected.sha256; \
    tar -xzf "$archive" --strip-components=1 -C /opt/deoos; \
    python -m pip install --no-cache-dir /opt/deoos/python/*.whl duckdb==1.5.6 boto3==1.42.97; \
    cd /app; \
    npm install --no-audit --no-fund /opt/deoos/node/*.tgz @duckdb/node-api@1.5.6-r.1; \
    rm -rf /tmp/release

COPY . /app/examples/
COPY docker_runner.sh /usr/local/bin/deoos-demo
RUN chmod +x /usr/local/bin/deoos-demo
WORKDIR /app
ENV PYTHONUNBUFFERED=1 ENGINE_BINARY=/opt/deoos/bin/deoos-server
ENTRYPOINT ["deoos-demo"]
