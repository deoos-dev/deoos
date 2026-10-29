PYTHON ?= python3
TEST_PYTHON ?= tests/.venv/bin/python
.PHONY: build setup test-local test-aws test-cluster package test-package test-filesystem clean-local dev dev-down test-dev

build:
	$(PYTHON) packaging/build_package.py --build-only

setup: build
	$(PYTHON) -m venv tests/.venv
	tests/.venv/bin/pip install -r tests/requirements.txt

test-local: build
	docker compose up -d
	ENGINE_BINARY="$(CURDIR)/engine/target/release/deoos-server" $(TEST_PYTHON) tests/two_modes.py rustfs
	ENGINE_BINARY="$(CURDIR)/engine/target/release/deoos-server" $(TEST_PYTHON) tests/storage_checks.py

test-aws: build
	ENGINE_BINARY="$(CURDIR)/engine/target/release/deoos-server" $(TEST_PYTHON) tests/two_modes.py aws

test-cluster: build
	docker compose -p durable-cluster -f tests/compose.cluster.yaml up -d
	$(TEST_PYTHON) tests/cluster_contract.py

package: build
	$(PYTHON) packaging/build_package.py --package-only

test-package:
	docker compose up -d
	$(TEST_PYTHON) tests/package_smoke.py

test-filesystem: package
	cargo test --offline --release --manifest-path engine/Cargo.toml --lib
	$(TEST_PYTHON) tests/package_smoke.py --backend filesystem
	$(TEST_PYTHON) tests/local_faults.py --binary engine/target/release/deoos-server --mounts

clean-local:
	docker compose down -v
	docker compose -p durable-cluster -f tests/compose.cluster.yaml down -v

dev:
	@PYTHON="$(PYTHON)" ./scripts/dev.sh up

dev-down:
	@PYTHON="$(PYTHON)" ./scripts/dev.sh down

test-dev:
	$(PYTHON) tests/dev_cli.py
