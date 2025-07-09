# DORA Metrics Tool - Detailed Implementation Plan

## Overview
This document outlines a step-by-step implementation plan for the DORA metrics back-calculation tool. Each step includes specific deliverables, testing requirements, and acceptance criteria to ensure thorough testing before moving to the next phase.

## Implementation Phases

### Phase 1: Project Setup and Infrastructure (Step 1)
**Goal**: Establish project structure and core utilities

**Deliverables**:
1. Project structure with all directories
2. `pyproject.toml` with dependencies
3. Basic logging setup
4. Storage manager abstraction (local filesystem only initially)

**Tests Required**:
- Unit tests for storage manager (read, write, exists, list operations)
- Test logging configuration
- Test project can be installed with pip

**Acceptance Criteria**:
- [ ] Project installs cleanly in a virtual environment
- [ ] Storage manager can read/write files locally
- [ ] Logging outputs to console with proper formatting
- [ ] All tests pass with 100% coverage for implemented modules

### Phase 2: Git Commit Extraction (Step 2)
**Goal**: Extract commit data from local git repositories

**Deliverables**:
1. `GitExtractor` class in `extractors/git_extractor.py`
2. Commit data model with all required fields
3. JSON serialization for commits

**Tests Required**:
- Unit tests with mock git repository
- Integration tests with a real test repository
- Test date filtering (since/until)
- Test branch filtering
- Test edge cases (empty repo, no commits in range)

**Acceptance Criteria**:
- [ ] Can extract all commits from a specified branch
- [ ] Date filtering works correctly
- [ ] Commits are properly serialized to JSON
- [ ] Performance: Can handle repos with 10k+ commits
- [ ] All tests pass with >90% coverage

### Phase 3: GitHub GraphQL Client (Step 3)
**Goal**: Fetch PR and release data from GitHub

**Deliverables**:
1. `GitHubGraphQLClient` class in `extractors/github_client.py`
2. GraphQL queries for PRs and releases
3. Data models for PullRequest and Deployment
4. Rate limiting and pagination handling

**Tests Required**:
- Unit tests with mocked GraphQL responses
- Test pagination handling
- Test rate limit handling
- Test error handling (network errors, auth errors)
- Integration tests with real GitHub API (using a test repo)

**Acceptance Criteria**:
- [ ] Can fetch all PRs in a date range
- [ ] Can fetch all releases in a date range
- [ ] Handles pagination automatically
- [ ] Respects rate limits with exponential backoff
- [ ] All tests pass with >90% coverage

### Phase 4: Data Association Logic (Step 4)
**Goal**: Associate commits with PRs and identify deployments

**Deliverables**:
1. `DataAssociator` class in `processors/data_associator.py`
2. Logic to match commits to PRs
3. Logic to identify deployment commits
4. Logic to identify hotfixes

**Tests Required**:
- Unit tests for association logic
- Test various PR/commit scenarios:
  - Single commit PR
  - Multi-commit PR
  - Commits without PRs
  - PRs without commits in main
- Test hotfix detection
- Test deployment detection

**Acceptance Criteria**:
- [ ] Correctly associates >95% of commits to their PRs
- [ ] Identifies deployments from releases
- [ ] Identifies hotfixes from PR labels
- [ ] Handles edge cases gracefully
- [ ] All tests pass with >90% coverage

### Phase 5: CSV Export and Import (Step 5)
**Goal**: Export data for manual review and import annotations

**Deliverables**:
1. `CSVHandler` class in `storage/csv_handler.py`
2. CSV export with all required columns
3. CSV import with validation
4. Annotation merger logic

**Tests Required**:
- Unit tests for CSV read/write
- Test CSV schema validation
- Test annotation merging
- Test handling of manual edits
- Test Unicode and special character handling

**Acceptance Criteria**:
- [ ] Exports readable CSV with all data
- [ ] Can import and validate annotated CSV
- [ ] Preserves manual annotations through updates
- [ ] Handles Excel-edited CSV files
- [ ] All tests pass with >90% coverage

### Phase 6: DORA Metrics Calculation (Step 6)
**Goal**: Calculate the four key DORA metrics

**Deliverables**:
1. Metrics calculation functions in `calculators/metrics.py`
2. Support for different time periods (daily, weekly, monthly)
3. JSON output of calculated metrics

**Tests Required**:
- Unit tests for each metric calculation
- Test with various data scenarios:
  - High-performing team data
  - Low-performing team data
  - Sparse data
  - Edge cases (no deployments, all failures)
- Test period aggregations

**Acceptance Criteria**:
- [ ] Lead time calculation matches manual calculations
- [ ] Deployment frequency is accurate
- [ ] Change failure rate handles edge cases
- [ ] MTTR calculation is correct
- [ ] All tests pass with >90% coverage

### Phase 7: CLI Interface (Step 7)
**Goal**: Create user-friendly command-line interface

**Deliverables**:
1. CLI commands in `cli.py` using Click
2. Progress indicators for long operations
3. Error handling and user-friendly messages
4. Help documentation

**Tests Required**:
- Unit tests for CLI commands
- Test argument parsing
- Test error handling
- Test progress indicators
- Integration tests for full workflows

**Acceptance Criteria**:
- [ ] All commands have helpful documentation
- [ ] Progress shown for operations >2 seconds
- [ ] Errors are caught and displayed clearly
- [ ] Commands can be chained in scripts
- [ ] All tests pass with >85% coverage

### Phase 8: Incremental Updates (Step 8)
**Goal**: Support incremental data updates

**Deliverables**:
1. Change detection logic
2. Incremental update command
3. Metadata tracking for last update

**Tests Required**:
- Unit tests for change detection
- Test incremental updates
- Test handling of force pushes
- Test metadata persistence
- Integration tests for update workflows

**Acceptance Criteria**:
- [ ] Detects new commits accurately
- [ ] Preserves existing annotations
- [ ] Handles repository changes gracefully
- [ ] Updates complete in <20% of full extraction time
- [ ] All tests pass with >85% coverage

### Phase 9: S3 Storage Support (Step 9)
**Goal**: Add S3 backend to storage manager

**Deliverables**:
1. S3 implementation in storage manager
2. Environment variable configuration
3. Clear error messages when S3 is unavailable

**Tests Required**:
- Unit tests with mocked boto3
- Integration tests with real S3 (or MinIO)
- Test fast fail behavior
- Test error handling (no credentials, bucket not found)

**Acceptance Criteria**:
- [ ] Can read/write to S3 when configured
- [ ] Fast fails with clear error when S3 not available
- [ ] Performance is acceptable (<2s overhead)
- [ ] All tests pass with >90% coverage

## Testing Strategy

### Test Pyramid
1. **Unit Tests** (70%): Fast, isolated tests for individual functions/classes
2. **Integration Tests** (20%): Test component interactions
3. **End-to-End Tests** (10%): Full workflow tests with real data

### Test Data Strategy
1. Create a `test_fixtures/` directory with:
   - Sample git repository (small)
   - Mock API responses
   - Sample CSV files
   - Known-good calculation results

2. Use factories for test data generation

### Continuous Testing
- Run tests before each commit
- Use pytest markers for test categories
- Separate slow integration tests
- Generate coverage reports

## Development Workflow

### For Each Phase:
1. Write failing tests first (TDD)
2. Implement minimal code to pass tests
3. Refactor for clarity and performance
4. Document code with docstrings
5. Update integration tests
6. Commit with descriptive message
7. Move to next phase only when all tests pass

### Code Review Checklist:
- [ ] All tests pass
- [ ] Coverage meets requirements
- [ ] Code follows Python conventions
- [ ] Docstrings are complete
- [ ] Error handling is appropriate
- [ ] Performance is acceptable

## Risk Mitigation

### Technical Risks:
1. **GitHub API changes**: Use versioned GraphQL schema
2. **Large repository performance**: Test with repos >10k commits early
3. **Memory usage**: Stream data where possible
4. **S3 costs**: Implement efficient caching

### Mitigation Strategies:
- Test with real-world repositories early
- Profile performance regularly
- Keep external dependencies minimal
- Design for extensibility from the start

## Success Metrics

### Phase Completion:
- All acceptance criteria met
- Test coverage targets achieved
- Performance benchmarks passed
- No critical bugs

### Overall Project:
- Can process a 10k commit repository in <5 minutes
- Metrics match manual calculations within 1%
- Tool is usable without documentation
- Can be deployed in CI/CD environment