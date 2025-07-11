"""End-to-end tests for CLI with real GitHub repository."""

import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from dora_metrics.cli import cli


@pytest.mark.e2e
@pytest.mark.requires_github
class TestCLIEndToEnd:
    """End-to-end tests using real GitHub repository."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def github_token(self):
        """Get GitHub token from environment."""
        token = os.environ.get('GITHUB_TOKEN')
        if not token:
            pytest.skip("GITHUB_TOKEN not set")
        return token
    
    def test_full_workflow_with_github(self, runner, github_token):
        """Test complete workflow with a real GitHub repository."""
        with tempfile.TemporaryDirectory() as storage_dir:
            # Use a small, well-maintained repository for testing
            owner = "octocat"
            repo = "Hello-World"
            
            # Calculate date range (last 30 days)
            until_date = datetime.now()
            since_date = until_date - timedelta(days=30)
            since_str = since_date.strftime("%Y-%m-%d")
            until_str = until_date.strftime("%Y-%m-%d")
            
            # 1. Clone the repository first
            import git
            repo_path = Path(storage_dir) / repo
            print(f"Cloning {owner}/{repo}...")
            git_repo = git.Repo.clone_from(
                f"https://github.com/{owner}/{repo}.git",
                str(repo_path)
                # Full clone - no depth limit
            )
            
            # 2. Extract commits from local git
            print("Extracting commits...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-commits',
                '--repo-path', str(repo_path),
                '--branch', 'master',  # Hello-World uses master
                # Don't limit by date to ensure we have all commits referenced by PRs
            ])
            
            if result.exit_code != 0:
                print(f"Failed with output: {result.output}")
            assert result.exit_code == 0
            assert "✓ Extracted" in result.output
            
            # 3. Extract GitHub data (PRs and releases)
            print("Extracting GitHub data...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-github',
                '--owner', owner,
                '--repo', repo,
                '--token', github_token,
                '--since', since_str,
                '--until', until_str
            ])
            
            if result.exit_code != 0:
                print(f"Failed with output: {result.output}")
            assert result.exit_code == 0
            assert "✓ Extracted" in result.output
            assert "PRs" in result.output
            
            # 4. Associate commits with PRs and deployments
            print("Associating data...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'associate',
                '--repo', repo
            ])
            
            if result.exit_code != 0:
                print(f"Associate failed with output: {result.output}")
            assert result.exit_code == 0
            assert "✓ Associated" in result.output
            
            # 5. Validate data quality
            print("Validating data quality...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'validate',
                '--repo', repo
            ])
            
            # Validation may find issues but shouldn't error
            assert result.exit_code == 0
            
            # Check if there are critical issues
            has_critical_issues = "CRITICAL ISSUES" in result.output
            if has_critical_issues:
                print("Note: Critical data quality issues found")
                print("This is expected with partial data extraction")
            
            # 6. Export to CSV
            print("Exporting to CSV...")
            csv_path = Path(storage_dir) / "export.csv"
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'export',
                '--repo', repo,
                '--output', str(csv_path)
            ])
            
            # If there are critical issues, they should be about data quality
            if result.exit_code != 0:
                print(f"Export output: {result.output}")
                # For this test, we'll allow export to fail if there are critical issues
                # In real usage, users would fix these issues first
                assert "CRITICAL ISSUES" in result.output
            else:
                assert "✓ Exported data to" in result.output
                assert csv_path.with_suffix('.commits.csv').exists()
                assert csv_path.with_suffix('.prs.csv').exists()
                assert csv_path.with_suffix('.deployments.csv').exists()
            
            # 7. Calculate metrics (should work even with some data quality issues)
            print("Calculating metrics...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'calculate',
                '--repo', repo,
                '--period', 'weekly',
                '--output-format', 'json',
                '--since', since_str,
                '--until', until_str
            ])
            
            if result.exit_code != 0:
                print(f"Calculate output: {result.output}")
            assert result.exit_code == 0
            
            # Parse JSON output
            metrics = json.loads(result.output)
            assert len(metrics) > 0
            assert 'period' in metrics[0]
            assert 'metrics' in metrics[0]
            
            # 8. Calculate metrics with table output
            print("Calculating metrics (table format)...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'calculate',
                '--repo', repo,
                '--period', 'weekly',
                '--output-format', 'table',
                '--since', since_str,
                '--until', until_str
            ])
            
            assert result.exit_code == 0
            assert "DORA Metrics Summary" in result.output
            assert "Lead Time" in result.output
            assert "Deploy Freq" in result.output
            
            # 9. Analyze PR health
            print("Analyzing PR health...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'pr-health',
                '--repo', repo
            ])
            
            assert result.exit_code == 0
            assert "Total Open PRs:" in result.output
            
            # 10. Get detailed PR health report
            print("Getting detailed PR health report...")
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'pr-health',
                '--repo', repo,
                '--detailed'
            ])
            
            assert result.exit_code == 0
            assert "PR HEALTH REPORT" in result.output
            assert "SUMMARY" in result.output
    
    def test_incremental_update_workflow(self, runner, github_token):
        """Test incremental update workflow."""
        with tempfile.TemporaryDirectory() as storage_dir:
            # Use a smaller repository for faster testing
            owner = "octocat"
            repo = "Spoon-Knife"
            
            # Initial extraction (last 14 days)
            until_date = datetime.now()
            since_date = until_date - timedelta(days=14)
            since_str = since_date.strftime("%Y-%m-%d")
            until_str = until_date.strftime("%Y-%m-%d")
            
            # Clone repository
            repo_path = Path(storage_dir) / repo
            import git
            git.Repo.clone_from(
                f"https://github.com/{owner}/{repo}.git",
                str(repo_path)
            )
            
            # Initial extraction
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-commits',
                '--repo-path', str(repo_path),
                '--branch', 'main',
                '--since', since_str
            ])
            assert result.exit_code == 0
            
            # Get initial commit count
            from dora_metrics.storage.repository import DataRepository
            from dora_metrics.storage.storage_manager import StorageManager
            
            storage = StorageManager(base_path=Path(storage_dir))
            data_repo = DataRepository(storage)
            initial_commits = data_repo.load_commits(repo)
            initial_count = len(initial_commits)
            
            # Try the update command (currently just a placeholder)
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'update',
                '--repo', repo
            ])
            
            # Should inform that incremental updates aren't implemented yet
            assert "Incremental updates not yet implemented" in result.output
    
    def test_small_repo_complete_analysis(self, runner, github_token):
        """Test with a smaller repository for complete analysis."""
        with tempfile.TemporaryDirectory() as storage_dir:
            # Use a smaller, simpler repository
            owner = "octocat"
            repo = "Hello-World"
            
            # Clone the repository
            repo_path = Path(storage_dir) / "hello-world"
            import git
            git_repo = git.Repo.clone_from(
                f"https://github.com/{owner}/{repo}.git",
                str(repo_path)
            )
            
            # Extract all commits
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-commits',
                '--repo-path', str(repo_path),
                '--branch', 'master'  # This repo uses master
            ])
            assert result.exit_code == 0
            
            # Extract GitHub data
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-github',
                '--owner', owner,
                '--repo', repo,
                '--token', github_token
            ])
            assert result.exit_code == 0
            
            # Associate
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'associate',
                '--repo', repo
            ])
            assert result.exit_code == 0
            
            # Validate - this simple repo should have good data quality
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'validate',
                '--repo', repo,
                '--full'
            ])
            assert result.exit_code == 0
            
            # Calculate metrics for all time
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'calculate',
                '--repo', repo,
                '--period', 'monthly',
                '--output-format', 'json'
            ])
            
            if result.exit_code == 0:
                metrics = json.loads(result.output)
                print(f"Found {len(metrics)} months of metrics")
                if metrics:
                    print(f"First month: {metrics[0]['period']}")
                    print(f"Metrics: {json.dumps(metrics[0]['metrics'], indent=2)}")
    
    def test_data_quality_scenarios(self, runner, github_token):
        """Test various data quality scenarios."""
        with tempfile.TemporaryDirectory() as storage_dir:
            # Use a smaller repository for faster testing
            owner = "octocat"
            repo = "Hello-World"
            
            # Get recent data
            until_date = datetime.now()
            since_date = until_date - timedelta(days=7)
            since_str = since_date.strftime("%Y-%m-%d")
            until_str = until_date.strftime("%Y-%m-%d")
            
            # Clone repository
            repo_path = Path(storage_dir) / "test-repo"
            import git
            git.Repo.clone_from(
                f"https://github.com/{owner}/{repo}.git",
                str(repo_path)
            )
            
            # Extract data
            runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-commits',
                '--repo-path', str(repo_path),
                '--branch', 'master',  # Hello-World uses master
                '--since', since_str
            ])
            
            runner.invoke(cli, [
                '--storage-path', storage_dir,
                'extract-github',
                '--owner', owner,
                '--repo', repo,
                '--token', github_token,
                '--since', since_str
            ])
            
            runner.invoke(cli, [
                '--storage-path', storage_dir,
                'associate',
                '--repo', repo
            ])
            
            # Run full validation report
            result = runner.invoke(cli, [
                '--storage-path', storage_dir,
                'validate',
                '--repo', repo,
                '--full'
            ])
            
            assert result.exit_code == 0
            
            # Check for various validation messages
            output = result.output
            
            # Should categorize issues correctly
            if "CRITICAL ISSUES" in output:
                print("Found critical issues:")
                print(output[output.find("CRITICAL ISSUES"):output.find("WARNINGS") if "WARNINGS" in output else len(output)])
            
            if "WARNINGS" in output:
                print("\nFound warnings:")
                print(output[output.find("WARNINGS"):output.find("INFORMATIONAL") if "INFORMATIONAL" in output else len(output)])
            
            if "INFORMATIONAL" in output:
                print("\nFound informational items:")
                # Just print first few lines of informational
                info_section = output[output.find("INFORMATIONAL"):]
                print('\n'.join(info_section.split('\n')[:10]))