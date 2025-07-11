"""Unit tests for CLI commands."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from dora_metrics.cli import cli
from dora_metrics.models import Commit, Deployment, PRState, PullRequest


@pytest.mark.unit
class TestCLI:
    """Test CLI commands."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_storage_manager(self):
        """Create a mock storage manager."""
        with patch('dora_metrics.cli.StorageManager') as mock_storage:
            with patch('dora_metrics.cli.DataRepository') as mock_repo:
                yield mock_storage, mock_repo
    
    @pytest.fixture
    def sample_commits(self):
        """Create sample commits."""
        return [
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
        ]
    
    @pytest.fixture
    def sample_prs(self):
        """Create sample PRs."""
        return [
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
        ]
    
    @pytest.fixture
    def sample_deployments(self):
        """Create sample deployments."""
        return [
            Deployment(
                tag_name="v1.0.0",
                name="Version 1.0.0",
                created_at=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                commit_sha="def456",
            ),
        ]
    
    def test_extract_commits(self, runner, mock_storage_manager):
        """Test extract-commits command."""
        with patch('dora_metrics.cli.GitExtractor') as mock_extractor:
            # Setup mocks
            mock_instance = mock_extractor.return_value
            mock_instance.extract_commits.return_value = []
            
            # Run command
            result = runner.invoke(cli, [
                'extract-commits',
                '--repo-path', '/path/to/repo',
                '--branch', 'main',
                '--since', '2024-01-01',
                '--until', '2024-01-31'
            ])
            
            # Check results
            assert result.exit_code == 0
            assert "Extracting commits" in result.output
            assert "✓ Extracted 0 commits" in result.output
            
            # Verify calls
            mock_extractor.assert_called_once_with('/path/to/repo')
            mock_instance.extract_commits.assert_called_once()
    
    def test_extract_github(self, runner, mock_storage_manager):
        """Test extract-github command."""
        with patch('dora_metrics.cli.GitHubGraphQLClient') as mock_client:
            # Setup mocks
            mock_instance = mock_client.return_value
            
            # Run command
            result = runner.invoke(cli, [
                'extract-github',
                '--owner', 'test-owner',
                '--repo', 'test-repo',
                '--token', 'test-token',
                '--since', '2024-01-01'
            ])
            
            # Check results
            assert result.exit_code == 0
            assert "Extracting PRs" in result.output
            assert "Extracting releases" in result.output
            
            # Verify calls
            mock_client.assert_called_once_with('test-token', 'test-owner', 'test-repo')
    
    def test_associate(self, runner, mock_storage_manager, sample_commits, sample_prs, sample_deployments):
        """Test associate command."""
        with patch('dora_metrics.cli.DataAssociator') as mock_associator:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_pull_requests.return_value = sample_prs
            mock_repo.load_deployments.return_value = sample_deployments
            
            mock_assoc_instance = mock_associator.return_value
            # The associate_data method returns (commits, prs)
            mock_assoc_instance.associate_data.return_value = (sample_commits, sample_prs)
            
            # Run command
            result = runner.invoke(cli, ['associate', '--repo', 'test-repo'])
            
            # Check results
            assert result.exit_code == 0
            assert "Loading data..." in result.output
            assert "Associating data..." in result.output
            assert "✓ Associated 2/2 commits with PRs" in result.output
            assert "✓ Identified 1 deployment commits" in result.output
    
    def test_export_with_critical_issues(self, runner, mock_storage_manager, sample_commits, sample_prs):
        """Test export command with critical data quality issues."""
        with patch('dora_metrics.cli.DataQualityValidator') as mock_validator:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_pull_requests.return_value = sample_prs
            mock_repo.load_deployments.return_value = []
            
            mock_validator_instance = mock_validator.return_value
            mock_report = MagicMock()
            mock_report.critical_issues = [{'message': 'PRs without commits found'}]
            mock_report.warnings = []
            mock_validator_instance.validate.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, [
                'export',
                '--repo', 'test-repo',
                '--output', '/tmp/test.csv'
            ])
            
            # Check results
            assert result.exit_code == 1
            assert "CRITICAL ISSUES" in result.output
            assert "PRs without commits found" in result.output
            assert "Export blocked" in result.output
    
    def test_export_success(self, runner, mock_storage_manager, sample_commits, sample_prs, sample_deployments):
        """Test successful export command."""
        with patch('dora_metrics.cli.DataQualityValidator') as mock_validator, \
             patch('dora_metrics.cli.CSVHandler') as mock_csv:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_pull_requests.return_value = sample_prs
            mock_repo.load_deployments.return_value = sample_deployments
            
            mock_validator_instance = mock_validator.return_value
            mock_report = MagicMock()
            mock_report.critical_issues = []
            mock_report.warnings = [{'message': 'Some commits without PRs'}]
            mock_validator_instance.validate.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, [
                'export',
                '--repo', 'test-repo',
                '--output', '/tmp/test.csv'
            ])
            
            # Check results
            assert result.exit_code == 0
            assert "WARNINGS" in result.output
            assert "Some commits without PRs" in result.output
            assert "✓ Exported data to" in result.output
            assert "Next steps:" in result.output
    
    def test_import_csv(self, runner, mock_storage_manager, sample_commits, sample_prs, sample_deployments):
        """Test import command."""
        with patch('dora_metrics.cli.DataQualityValidator') as mock_validator, \
             patch('dora_metrics.cli.CSVHandler') as mock_csv, \
             patch('pathlib.Path.exists', return_value=True):
            # Setup mocks
            mock_csv_instance = mock_csv.return_value
            mock_csv_instance.import_commits.return_value = sample_commits
            mock_csv_instance.import_pull_requests.return_value = sample_prs
            mock_csv_instance.import_deployments.return_value = sample_deployments
            
            mock_validator_instance = mock_validator.return_value
            mock_report = MagicMock()
            mock_report.critical_issues = []
            mock_report.warnings = []
            mock_validator_instance.validate.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, [
                'import',
                '--repo', 'test-repo',
                '--input', '/tmp/test.csv'
            ])
            
            # Check results
            assert result.exit_code == 0
            assert "Importing data from" in result.output
            assert "✓ Imported 2 commits, 2 PRs, 1 deployments" in result.output
    
    def test_calculate_json_output(self, runner, mock_storage_manager, sample_commits, sample_deployments):
        """Test calculate command with JSON output."""
        with patch('dora_metrics.cli.MetricsCalculator') as mock_calculator:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_deployments.return_value = sample_deployments
            
            mock_calc_instance = mock_calculator.return_value
            mock_metrics = MagicMock()
            mock_metrics.to_dict.return_value = {
                'lead_time_p50': 10.5,
                'deployment_frequency': 2.0,
                'change_failure_rate': 0.1,
                'mttr_hours': 2.5
            }
            mock_calc_instance.calculate_weekly_metrics.return_value = {
                '2024-W01': mock_metrics
            }
            
            # Run command
            result = runner.invoke(cli, [
                'calculate',
                '--repo', 'test-repo',
                '--period', 'weekly',
                '--output-format', 'json'
            ])
            
            # Check results
            assert result.exit_code == 0
            output = json.loads(result.output)
            assert len(output) == 1
            assert output[0]['period'] == '2024-W01'
            assert output[0]['metrics']['lead_time_p50'] == 10.5
    
    def test_calculate_table_output(self, runner, mock_storage_manager, sample_commits, sample_deployments):
        """Test calculate command with table output."""
        with patch('dora_metrics.cli.MetricsCalculator') as mock_calculator:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_deployments.return_value = sample_deployments
            
            mock_metrics = MagicMock()
            mock_metrics.lead_time_p50 = 10.5
            mock_metrics.lead_time_p90 = 20.0
            mock_metrics.deployment_frequency = 2.0
            mock_metrics.change_failure_rate = 0.1
            mock_metrics.mean_time_to_restore = 2.5
            
            mock_calc_instance = mock_calculator.return_value
            mock_calc_instance.calculate_weekly_metrics.return_value = {
                '2024-W01': mock_metrics
            }
            
            # Run command
            result = runner.invoke(cli, [
                'calculate',
                '--repo', 'test-repo',
                '--period', 'weekly'
            ])
            
            # Check results
            assert result.exit_code == 0
            assert "DORA Metrics Summary" in result.output
            assert "2024-W01" in result.output
            assert "10.5h" in result.output  # Lead time p50
            assert "2.0/day" in result.output  # Deploy frequency
            assert "Performance Level" in result.output
            assert "Elite" in result.output
    
    def test_validate(self, runner, mock_storage_manager, sample_commits, sample_prs):
        """Test validate command."""
        with patch('dora_metrics.cli.DataQualityValidator') as mock_validator:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_commits.return_value = sample_commits
            mock_repo.load_pull_requests.return_value = sample_prs
            mock_repo.load_deployments.return_value = []
            
            mock_validator_instance = mock_validator.return_value
            mock_report = MagicMock()
            mock_report.critical_issues = []
            mock_report.warnings = [{'message': 'Some warning', 'details': ['Detail 1', 'Detail 2']}]
            mock_report.informational = [{'message': 'Some info'}]
            mock_validator_instance.validate.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, ['validate', '--repo', 'test-repo'])
            
            # Check results
            assert result.exit_code == 0
            assert "WARNINGS" in result.output
            assert "Some warning" in result.output
            assert "Some warnings found, but you can proceed" in result.output
    
    def test_pr_health(self, runner, mock_storage_manager, sample_prs):
        """Test pr-health command."""
        with patch('dora_metrics.cli.PRHealthAnalyzer') as mock_analyzer:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_pull_requests.return_value = sample_prs
            
            mock_analyzer_instance = mock_analyzer.return_value
            mock_report = MagicMock()
            mock_report.get_summary.return_value = "Total Open PRs: 0"
            mock_report.recommendations = ["Do something"]
            mock_analyzer_instance.analyze.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, ['pr-health', '--repo', 'test-repo'])
            
            # Check results
            assert result.exit_code == 0
            assert "Total Open PRs: 0" in result.output
            assert "RECOMMENDATIONS" in result.output
            assert "Do something" in result.output
    
    def test_pr_health_detailed(self, runner, mock_storage_manager, sample_prs):
        """Test pr-health command with detailed flag."""
        with patch('dora_metrics.cli.PRHealthAnalyzer') as mock_analyzer:
            # Setup mocks
            mock_storage, mock_repo_class = mock_storage_manager
            mock_repo = mock_repo_class.return_value
            mock_repo.load_pull_requests.return_value = sample_prs
            
            mock_analyzer_instance = mock_analyzer.return_value
            mock_report = MagicMock()
            mock_report.get_detailed_report.return_value = "PR HEALTH REPORT\n======"
            mock_analyzer_instance.analyze.return_value = mock_report
            
            # Run command
            result = runner.invoke(cli, ['pr-health', '--repo', 'test-repo', '--detailed'])
            
            # Check results
            assert result.exit_code == 0
            assert "PR HEALTH REPORT" in result.output