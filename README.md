# DORA Metrics Back-Calculation Tool

A tool for calculating DORA (DevOps Research and Assessment) metrics from GitHub repositories.

## Features

- Extract commit history from git repositories
- Fetch PR and release data from GitHub using GraphQL API
- Associate commits with pull requests and deployments
- Export data for manual annotation
- Calculate the four key DORA metrics:
  - Lead Time for Changes
  - Deployment Frequency
  - Change Failure Rate
  - Mean Time to Restore (MTTR)

## Installation

```bash
pip install -e ".[dev]"
```

## Usage

```bash
# Extract data from a repository
dora-metrics extract --repo ./path/to/repo --since 2024-01-01

# Import annotated data
dora-metrics import annotations.csv

# Calculate metrics
dora-metrics calculate --period monthly

# Update with new data
dora-metrics update
```

## Development

Run tests:
```bash
pytest
```

Run with coverage:
```bash
pytest --cov=dora_metrics
```

Format code:
```bash
black src tests
ruff check src tests
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## Implementation Plan

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for the development roadmap.