"""Integration tests for CLI commands."""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from dora_metrics.cli import cli
from dora_metrics.models import Commit, Deployment, PRState, PullRequest
from dora_metrics.storage.repository import DataRepository
from dora_metrics.storage.storage_manager import StorageManager


@pytest.mark.integration
class TestCLIIntegration:
    """Test CLI commands with real storage operations."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_storage(self):
        """Create a temporary storage directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        commits = [
            Commit(
                sha="abc123",
                author_name="Alice",
                author_email="alice@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Alice",
                committer_email="alice@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Add feature",
                pr_number=1,
            ),
            Commit(
                sha="def456",
                author_name="Bob",
                author_email="bob@example.com",
                authored_date=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                committer_name="Bob",
                committer_email="bob@example.com",
                committed_date=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                message="Fix bug",
                pr_number=2,
                deployment_tag="v1.0.0",
            ),
            Commit(
                sha="ghi789",
                author_name="Charlie",
                author_email="charlie@example.com",
                authored_date=datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
                committer_name="Charlie",
                committer_email="charlie@example.com",
                committed_date=datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
                message="Update docs",
                pr_number=None,  # Direct push
            ),
        ]
        
        prs = [
            PullRequest(
                number=1,
                title="Add feature",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="abc123",
                author="alice",
                commits=["abc123"],
                additions=50,
                deletions=10,
            ),
            PullRequest(
                number=2,
                title="Fix bug",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                merge_commit_sha="def456",
                author="bob",
                commits=["def456"],
                additions=20,
                deletions=5,
            ),
            PullRequest(
                number=3,
                title="Work in progress",
                state=PRState.OPEN,
                created_at=datetime(2024, 1, 3, 13, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 3, 14, 0, tzinfo=timezone.utc),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="charlie",
                commits=[],  # No commits yet - this is fine
                additions=0,
                deletions=0,
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Version 1.0.0",
                created_at=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                commit_sha="def456",
            ),
        ]
        
        return commits, prs, deployments
    
    def test_export_import_workflow(self, runner, temp_storage, sample_data):
        """Test export and import workflow."""
        commits, prs, deployments = sample_data
        
        # Save sample data to storage
        storage = StorageManager(base_path=Path(temp_storage))
        repo = DataRepository(storage)
        repo.save_commits("test-repo", commits)
        repo.save_pull_requests("test-repo", prs)
        repo.save_deployments("test-repo", deployments)
        
        # Export to CSV
        csv_path = Path(temp_storage) / "export.csv"
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'export',
            '--repo', 'test-repo',
            '--output', str(csv_path)
        ])
        
        # Export should succeed - PR validation is no longer done
        assert result.exit_code == 0
        assert "✓ Exported data to" in result.output
        # Check that the CSV files were created
        assert csv_path.with_suffix('.commits.csv').exists()
        assert csv_path.with_suffix('.prs.csv').exists()
        assert csv_path.with_suffix('.deployments.csv').exists()
        
        # Test import
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'import',
            '--repo', 'test-repo-2',
            '--input', str(csv_path)
        ])
        
        assert result.exit_code == 0
        assert "✓ Imported 3 commits, 3 PRs, 1 deployments" in result.output
        
        # Verify imported data
        imported_commits = repo.load_commits("test-repo-2")
        assert len(imported_commits) == 3
    
    def test_calculate_metrics_workflow(self, runner, temp_storage, sample_data):
        """Test metrics calculation workflow."""
        commits, prs, deployments = sample_data
        
        # Save sample data
        storage = StorageManager(base_path=Path(temp_storage))
        repo = DataRepository(storage)
        repo.save_commits("test-repo", commits)
        repo.save_pull_requests("test-repo", prs)
        repo.save_deployments("test-repo", deployments)
        
        # Calculate weekly metrics for the sample data period
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'calculate',
            '--repo', 'test-repo',
            '--period', 'weekly',
            '--output-format', 'table',
            '--since', '2024-01-01',
            '--until', '2024-01-07'
        ])
        
        assert result.exit_code == 0
        assert "DORA Metrics Summary" in result.output
        assert "2024-W01" in result.output
        assert "Lead Time" in result.output
        assert "Deploy Freq" in result.output
        
        # Test JSON output
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'calculate',
            '--repo', 'test-repo',
            '--period', 'weekly',
            '--output-format', 'json',
            '--since', '2024-01-01',
            '--until', '2024-01-07'
        ])
        
        assert result.exit_code == 0
        metrics = json.loads(result.output)
        assert len(metrics) == 1
        assert metrics[0]['period'] == '2024-W01'
        assert 'metrics' in metrics[0]
    
    def test_validate_workflow(self, runner, temp_storage, sample_data):
        """Test validation workflow."""
        commits, prs, deployments = sample_data
        
        # Add a deployment that references a non-existent commit (critical issue)
        deployments.append(
            Deployment(
                tag_name="v2.0.0",
                name="Version 2.0.0",
                created_at=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                commit_sha="nonexistent123",  # This commit doesn't exist - critical!
            )
        )
        
        # Save data
        storage = StorageManager(base_path=Path(temp_storage))
        repo = DataRepository(storage)
        repo.save_commits("test-repo", commits)
        repo.save_pull_requests("test-repo", prs)
        repo.save_deployments("test-repo", deployments)
        
        # Run validation
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'validate',
            '--repo', 'test-repo'
        ])
        
        assert result.exit_code == 0  # Validate command itself succeeds
        assert "CRITICAL ISSUES" in result.output
        assert "references non-existent commit" in result.output
        assert "Critical issues must be fixed" in result.output
        
        # Run with full report
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'validate',
            '--repo', 'test-repo',
            '--full'
        ])
        
        assert result.exit_code == 0
        assert "INFORMATIONAL" in result.output or "WARNINGS" in result.output
    
    def test_pr_health_workflow(self, runner, temp_storage):
        """Test PR health analysis workflow."""
        # Create PRs with various health states
        prs = [
            # Active PR
            PullRequest(
                number=1,
                title="Active feature",
                state=PRState.OPEN,
                created_at=datetime(2024, 1, 10, 10, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 14, 10, 0, tzinfo=timezone.utc),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="alice",
                commits=["c1"],
                additions=100,
                deletions=50,
            ),
            # Stale PR
            PullRequest(
                number=2,
                title="Stale bugfix",
                state=PRState.OPEN,
                created_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 5, 10, 0, tzinfo=timezone.utc),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="bob",
                commits=["c2"],
                additions=200,
                deletions=100,
            ),
            # Abandoned PR
            PullRequest(
                number=3,
                title="Abandoned refactor",
                state=PRState.OPEN,
                created_at=datetime(2023, 11, 1, 10, 0, tzinfo=timezone.utc),
                updated_at=datetime(2023, 11, 15, 10, 0, tzinfo=timezone.utc),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="charlie",
                commits=["c3"],
                additions=1000,
                deletions=500,
            ),
        ]
        
        # Save PRs
        storage = StorageManager(base_path=Path(temp_storage))
        repo = DataRepository(storage)
        repo.save_pull_requests("test-repo", prs)
        
        # Run PR health analysis as of mid-January 2024
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'pr-health',
            '--repo', 'test-repo',
            '--as-of', '2024-01-15'
        ])
        
        if result.exit_code != 0:
            print(f"Command failed with output: {result.output}")
        assert result.exit_code == 0
        assert "Total Open PRs: 3" in result.output
        assert "Active: 1" in result.output
        assert "Stale: 1" in result.output
        assert "Abandoned: 1" in result.output
        assert "RECOMMENDATIONS" in result.output
        
        # Run detailed report
        result = runner.invoke(cli, [
            '--storage-path', temp_storage,
            'pr-health',
            '--repo', 'test-repo',
            '--detailed',
            '--as-of', '2024-01-15'
        ])
        
        assert result.exit_code == 0
        assert "PR HEALTH REPORT" in result.output
        assert "SIZE DISTRIBUTION" in result.output
        assert "AGE STATISTICS" in result.output