.PHONY: test lint format typecheck check

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run ty check

check:
	@./scripts/check.sh
