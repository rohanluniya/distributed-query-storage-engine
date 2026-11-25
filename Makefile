.PHONY: setup protos ingest run daemon stop lint typecheck test test-fast test-chaos benchmark licenses sbom screenshots clean

setup:
	uv sync --frozen

protos:
	uv run python scripts/compile_protos.py

ingest:
	uv run python -m services.cli ingest --scale local --seed 42 --report-out results/dq/dq_report.json

run:
	./scripts/run_portable.sh

daemon:
	./scripts/run_portable.sh --daemon

stop:
	@for f in .local_data/*.pid; do [ -f "$$f" ] && kill $$(cat "$$f") 2>/dev/null || true; rm -f "$$f"; done

lint:
	uv run ruff check .

typecheck:
	uv run mypy ingestion models processing services

test-fast:
	uv run pytest tests/unit tests/contracts

test-chaos:
	uv run pytest tests/chaos

test:
	uv run pytest --junitxml=results/tests/junit.xml

benchmark:
	uv run python benchmarks/run_benchmarks.py

licenses:
	uv run python scripts/check_licenses.py

sbom:
	uv run python scripts/generate_sbom.py

screenshots:
	./scripts/capture_screenshots.sh

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache .hypothesis
