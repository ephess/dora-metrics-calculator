# Detect if we're in a virtual environment
VENV_EXISTS := $(shell test -d venv && echo "yes")
ifeq ($(VENV_EXISTS), yes)
    VENV_ACTIVATE := . venv/bin/activate &&
else
    VENV_ACTIVATE :=
endif

.PHONY: help venv install test test-unit test-integration coverage coverage-html lint format clean

help:  ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

venv:  ## Create virtual environment
	python3 -m venv venv
	$(VENV_ACTIVATE) pip install --upgrade pip

install: venv  ## Install the project in development mode
	$(VENV_ACTIVATE) pip install -e ".[dev]"

test:  ## Run all tests
	$(VENV_ACTIVATE) pytest tests/ -v

test-unit:  ## Run unit tests only
	$(VENV_ACTIVATE) pytest tests/unit/ -v

test-integration:  ## Run integration tests only
	$(VENV_ACTIVATE) pytest tests/integration/ -v

coverage:  ## Run tests with coverage report
	$(VENV_ACTIVATE) pytest tests/ --cov=dora_metrics --cov-report=term-missing --cov-report=xml --cov-report=html

coverage-html: coverage  ## Open HTML coverage report in browser
	open htmlcov/index.html

lint:  ## Run linting checks
	$(VENV_ACTIVATE) ruff check src tests
	$(VENV_ACTIVATE) mypy src

format:  ## Format code with black
	$(VENV_ACTIVATE) black src tests
	$(VENV_ACTIVATE) ruff check --fix src tests

clean:  ## Clean up generated files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete