.PHONY: all build test fmt setup-hooks bench clean contract

all: build

contract:
	python3 tools/generate_contracts.py

build: contract
	./build.sh

fmt:
	@echo "==> Formatting Rust code..."
	(cd rust && cargo fmt)
	@echo "==> Formatting Go code..."
	GOTOOLCHAIN=go1.25.9 go fmt ./...
	@echo "==> Formatting Python code..."
	ruff format python/
	ruff check --fix python/

setup-hooks:
	@echo "==> Installing Git Pre-Commit Hook..."
	chmod +x .githooks/pre-commit
	cp .githooks/pre-commit .git/hooks/pre-commit
	@echo "✔ Git Pre-Commit Hook successfully installed!"

test:
	@echo "==> Running Go Tests..."
	GOTOOLCHAIN=go1.25.9 go test ./...
	@echo "==> Running Rust Tests..."
	cd rust && cargo test --workspace --locked
	@echo "==> Running Python Tests..."
	PYTHONPATH=python python3 -m unittest discover -s python/tests -p "test_*.py"

bench:
	python3 tools/run_benchmarks.py

wheel: contract
	pip install maturin
	maturin build --release --manifest-path rust/crates/bindings-py/Cargo.toml --out dist/

clean:
	rm -rf bin/
	rm -rf dist/
	rm -rf rust/target/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
