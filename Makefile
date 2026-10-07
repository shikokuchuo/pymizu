.DEFAULT_GOAL := help

.PHONY: help install test lint check-format type-check check build docs \
	docs-preview clean clean-build clean-pyc clean-test

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## pip install -e ".[dev]"
	pip install -e ".[dev]"

test: ## pytest with coverage
	coverage run -m pytest tests/
	coverage combine
	coverage report

lint: ## ruff format + ruff check --fix
	ruff format python tests benchmarks
	ruff check --fix python tests benchmarks

check-format: ## ruff format --check + ruff check
	ruff format --check python tests benchmarks
	ruff check python tests benchmarks

type-check: ## pyrefly check
	pyrefly check

check: lint type-check test ## the pre-push gate

build: ## python -m build
	python -m build

docs: ## great-docs build
	great-docs build

docs-preview: ## great-docs preview
	great-docs preview

clean: clean-build clean-pyc clean-test ## clean-build clean-pyc clean-test

clean-build:
	rm -rf build/ dist/ *.egg-info python/*.egg-info

clean-pyc:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.py[co]" -delete

clean-test:
	rm -rf .pytest_cache/ .coverage* coverage.xml
