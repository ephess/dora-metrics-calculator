"""Unit tests for CSV handler."""

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dora_metrics.models import Commit, Deployment, PRState, PullRequest
from dora_metrics.storage.csv_handler import CSVHandler


@pytest.mark.unit
class TestCSVHandler:
    """Test CSV handler functionality."""
    
    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for CSV files."""
        return tmp_path
        
    @pytest.fixture
    def sample_commits(self):
        """Create sample commits for testing."""
        return [
            Commit(
                sha="abc123",
                author_name="Dev One",
                author_email="dev1@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev One",
                committer_email="dev1@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Fix: Critical bug\nThis fixes the login issue",
                files_changed=["auth.py", "tests/test_auth.py"],
                additions=25,
                deletions=10,
                pr_number=123,
                deployment_tag="v1.0.0",
            ),
            Commit(
                sha="def456",
                author_name="Dev Two",
                author_email="dev2@example.com",
                authored_date=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                committer_name="Dev Two",
                committer_email="dev2@example.com",
                committed_date=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                message="Feature: Add dashboard",
                files_changed=["dashboard.py"],
                additions=100,
                deletions=0,
                pr_number=124,
                deployment_tag=None,
            ),
        ]
        
    @pytest.fixture
    def sample_prs(self):
        """Create sample pull requests for testing."""
        return [
            PullRequest(
                number=123,
                title="Fix: Critical login bug",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="abc123",
                commits=["abc123"],
                author="dev1",
                labels=["bug", "urgent"],
            ),
            PullRequest(
                number=124,
                title="Feature: Dashboard",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 13, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                merge_commit_sha="def456",
                commits=["def456"],
                author="dev2",
                labels=["enhancement"],
            ),
        ]
        
    @pytest.fixture
    def sample_deployments(self):
        """Create sample deployments for testing."""
        return [
            Deployment(
                tag_name="v1.0.0",
                name="Release 1.0.0",
                created_at=datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 1, 11, 30, tzinfo=timezone.utc),
                commit_sha="abc123",
                is_prerelease=False,
            ),
            Deployment(
                tag_name="v1.1.0-beta",
                name="Beta Release 1.1.0",
                created_at=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                published_at=None,
                commit_sha="ghi789",
                is_prerelease=True,
            ),
        ]
        
    def test_export_commits(self, temp_dir, sample_commits):
        """Test exporting commits to CSV."""
        handler = CSVHandler()
        csv_path = temp_dir / "commits.csv"
        
        handler.export_commits(sample_commits, csv_path)
        
        # Verify file exists
        assert csv_path.exists()
        
        # Read and verify content
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        assert len(rows) == 2
        
        # Check first commit
        assert rows[0]["sha"] == "abc123"
        assert rows[0]["author_name"] == "Dev One"
        assert rows[0]["message"] == "Fix: Critical bug This fixes the login issue"  # Newline replaced
        assert rows[0]["files_changed"] == "auth.py|tests/test_auth.py"
        assert rows[0]["pr_number"] == "123"
        assert rows[0]["deployment_tag"] == "v1.0.0"
        assert rows[0]["is_manual_deployment"] == ""
        assert rows[0]["manual_deployment_timestamp"] == ""
        assert rows[0]["manual_deployment_failed"] == ""
        
    def test_export_pull_requests_with_hotfix_detection(self, temp_dir, sample_prs):
        """Test exporting PRs with auto-detected hotfix status."""
        handler = CSVHandler()
        csv_path = temp_dir / "prs.csv"
        
        handler.export_pull_requests(sample_prs, csv_path)
        
        # Read and verify content
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        assert len(rows) == 2
        
        # PR 123 has "urgent" label - should be detected as hotfix
        assert rows[0]["number"] == "123"
        assert rows[0]["is_hotfix"] == "true"
        assert rows[0]["labels"] == "bug|urgent"
        
        # PR 124 has no hotfix labels
        assert rows[1]["number"] == "124"
        assert rows[1]["is_hotfix"] == "false"
        
    def test_export_deployments(self, temp_dir, sample_deployments):
        """Test exporting deployments to CSV."""
        handler = CSVHandler()
        csv_path = temp_dir / "deployments.csv"
        
        handler.export_deployments(sample_deployments, csv_path)
        
        # Read and verify content
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        assert len(rows) == 2
        
        assert rows[0]["tag_name"] == "v1.0.0"
        assert rows[0]["is_prerelease"] == "false"
        assert rows[0]["published_at"] != ""
        
        assert rows[1]["tag_name"] == "v1.1.0-beta"
        assert rows[1]["is_prerelease"] == "true"
        assert rows[1]["published_at"] == ""  # None becomes empty string
        
    def test_import_commits_with_annotations(self, temp_dir):
        """Test importing commits with manual annotations."""
        csv_path = temp_dir / "annotated_commits.csv"
        
        # Write CSV with annotations
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.COMMIT_COLUMNS)
            writer.writeheader()
            writer.writerow({
                "sha": "manual123",
                "author_name": "Dev",
                "author_email": "dev@example.com",
                "authored_date": "2024-01-05T10:00:00+00:00",
                "committer_name": "Dev",
                "committer_email": "dev@example.com",
                "committed_date": "2024-01-05T10:00:00+00:00",
                "message": "Deploy to production",
                "files_changed": "deploy.sh",
                "additions": "5",
                "deletions": "2",
                "pr_number": "",
                "deployment_tag": "",
                "is_manual_deployment": "true",
                "manual_deployment_timestamp": "2024-01-05T15:00:00+00:00",
                "manual_deployment_failed": "false",
                "notes": "Manual production deployment",
            })
            
        handler = CSVHandler()
        commits = handler.import_commits(csv_path)
        
        assert len(commits) == 1
        commit = commits[0]
        
        assert commit.sha == "manual123"
        assert commit.is_manual_deployment is True
        assert commit.manual_deployment_timestamp == datetime(2024, 1, 5, 15, 0, tzinfo=timezone.utc)
        assert commit.manual_deployment_failed is False
        assert commit.notes == "Manual production deployment"
        
    def test_import_commits_manual_deployment_failed_parsing(self, temp_dir):
        """Test that manual_deployment_failed is correctly parsed as boolean."""
        handler = CSVHandler()
        csv_content = """sha,author_name,author_email,authored_date,committer_name,committer_email,committed_date,message,files_changed,additions,deletions,pr_number,deployment_tag,is_manual_deployment,manual_deployment_timestamp,manual_deployment_failed,notes
abc123,Alice,alice@example.com,2024-01-01T10:00:00Z,Alice,alice@example.com,2024-01-01T10:00:00Z,Deploy v1.0,file1.py,10,5,1,v1.0,true,2024-01-01T12:00:00Z,false,Initial deployment
def456,Bob,bob@example.com,2024-01-02T10:00:00Z,Bob,bob@example.com,2024-01-02T10:00:00Z,Deploy v1.1,file2.py,20,10,2,v1.1,true,2024-01-02T12:00:00Z,true,Failed deployment
ghi789,Charlie,charlie@example.com,2024-01-03T10:00:00Z,Charlie,charlie@example.com,2024-01-03T10:00:00Z,Deploy v1.2,file3.py,30,15,3,v1.2,true,2024-01-03T12:00:00Z,FALSE,Successful deployment
"""
        csv_file = temp_dir / "test_failed_parsing.csv"
        csv_file.write_text(csv_content)
        
        commits = handler.import_commits(csv_file)
        
        assert len(commits) == 3
        
        # First commit - manual_deployment_failed = "false"
        assert commits[0].sha == "abc123"
        assert commits[0].is_manual_deployment is True
        assert commits[0].manual_deployment_failed is False  # Should be boolean False
        assert isinstance(commits[0].manual_deployment_failed, bool)
        
        # Second commit - manual_deployment_failed = "true"
        assert commits[1].sha == "def456"
        assert commits[1].is_manual_deployment is True
        assert commits[1].manual_deployment_failed is True  # Should be boolean True
        assert isinstance(commits[1].manual_deployment_failed, bool)
        
        # Third commit - manual_deployment_failed = "FALSE"
        assert commits[2].sha == "ghi789"
        assert commits[2].is_manual_deployment is True
        assert commits[2].manual_deployment_failed is False  # Should be boolean False
        assert isinstance(commits[2].manual_deployment_failed, bool)

    def test_import_commits_default_timestamp(self, temp_dir):
        """Test that manual deployment timestamp defaults to commit timestamp."""
        csv_path = temp_dir / "commits.csv"
        
        # Write CSV without manual timestamp
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.COMMIT_COLUMNS)
            writer.writeheader()
            writer.writerow({
                "sha": "abc123",
                "author_name": "Dev",
                "author_email": "dev@example.com",
                "authored_date": "2024-01-05T10:00:00+00:00",
                "committer_name": "Dev",
                "committer_email": "dev@example.com",
                "committed_date": "2024-01-05T11:00:00+00:00",
                "message": "Fix",
                "files_changed": "",
                "additions": "0",
                "deletions": "0",
                "pr_number": "",
                "deployment_tag": "",
                "is_manual_deployment": "yes",  # Test alternative boolean
                "manual_deployment_timestamp": "",  # Empty
                "manual_deployment_failed": "",
                "notes": "",
            })
            
        handler = CSVHandler()
        commits = handler.import_commits(csv_path)
        
        commit = commits[0]
        assert commit.is_manual_deployment is True
        # Should default to committed_date
        assert commit.manual_deployment_timestamp == datetime(2024, 1, 5, 11, 0, tzinfo=timezone.utc)
        
    def test_import_pull_requests_with_modified_hotfix(self, temp_dir):
        """Test importing PRs where human modified hotfix status."""
        csv_path = temp_dir / "prs.csv"
        
        # Write CSV with human-modified hotfix status
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.PR_COLUMNS)
            writer.writeheader()
            writer.writerow({
                "number": "125",
                "title": "Quick fix",
                "state": "MERGED",
                "created_at": "2024-01-03T10:00:00+00:00",
                "updated_at": "2024-01-03T11:00:00+00:00",
                "closed_at": "2024-01-03T11:00:00+00:00",
                "merged_at": "2024-01-03T11:00:00+00:00",
                "merge_commit_sha": "xyz789",
                "commits": "xyz789",
                "author": "dev3",
                "labels": "bug",  # No hotfix label
                "is_hotfix": "true",  # But human marked as hotfix
                "notes": "Actually was emergency fix",
            })
            
        handler = CSVHandler()
        prs = handler.import_pull_requests(csv_path)
        
        assert len(prs) == 1
        pr = prs[0]
        
        assert pr.number == 125
        assert pr.is_hotfix is True  # Human override
        assert pr.notes == "Actually was emergency fix"
        
    def test_import_deployments_with_failure_info(self, temp_dir):
        """Test importing deployments with failure annotations."""
        csv_path = temp_dir / "deployments.csv"
        
        # Write CSV with failure info
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.DEPLOYMENT_COLUMNS)
            writer.writeheader()
            writer.writerow({
                "tag_name": "v1.2.0",
                "name": "Failed Release",
                "created_at": "2024-01-10T10:00:00+00:00",
                "published_at": "2024-01-10T10:30:00+00:00",
                "commit_sha": "fail123",
                "is_prerelease": "no",
                "deployment_failed": "yes",
                "failure_resolved_at": "2024-01-10T12:00:00+00:00",
                "notes": "Database migration failed",
            })
            
        handler = CSVHandler()
        deployments = handler.import_deployments(csv_path)
        
        assert len(deployments) == 1
        deployment = deployments[0]
        
        assert deployment.tag_name == "v1.2.0"
        assert deployment.deployment_failed is True
        assert deployment.failure_resolved_at == datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
        assert deployment.notes == "Database migration failed"
        
    def test_boolean_parsing(self):
        """Test various boolean value formats."""
        handler = CSVHandler()
        
        # True values
        assert handler._parse_bool("true") is True
        assert handler._parse_bool("TRUE") is True
        assert handler._parse_bool("yes") is True
        assert handler._parse_bool("YES") is True
        assert handler._parse_bool("y") is True
        assert handler._parse_bool("1") is True
        assert handler._parse_bool("x") is True
        
        # False values
        assert handler._parse_bool("false") is False
        assert handler._parse_bool("FALSE") is False
        assert handler._parse_bool("no") is False
        assert handler._parse_bool("n") is False
        assert handler._parse_bool("0") is False
        
        # None values
        assert handler._parse_bool("") is None
        assert handler._parse_bool("  ") is None
        
    def test_datetime_parsing(self):
        """Test various datetime formats."""
        handler = CSVHandler()
        
        # ISO format
        dt1 = handler._parse_datetime("2024-01-01T10:00:00+00:00")
        assert dt1.year == 2024
        assert dt1.hour == 10
        
        # With Z suffix
        dt2 = handler._parse_datetime("2024-01-01T10:00:00Z")
        assert dt2.tzinfo is not None
        
        # Other formats via dateutil
        dt3 = handler._parse_datetime("2024/01/01 10:00:00")
        assert dt3.year == 2024
        
        # Error on empty
        with pytest.raises(ValueError):
            handler._parse_datetime("")
            
    def test_custom_hotfix_labels(self, temp_dir):
        """Test using custom hotfix labels."""
        handler = CSVHandler(hotfix_labels={"emergency", "hotfix", "patch"})
        
        prs = [
            PullRequest(
                number=200,
                title="Patch",
                state=PRState.MERGED,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                closed_at=datetime.now(timezone.utc),
                merged_at=datetime.now(timezone.utc),
                merge_commit_sha="patch123",
                commits=["patch123"],
                author="dev",
                labels=["patch", "bug"],  # patch is in custom labels
            ),
        ]
        
        csv_path = temp_dir / "prs.csv"
        handler.export_pull_requests(prs, csv_path)
        
        # Read back
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        assert rows[0]["is_hotfix"] == "true"
        
    def test_unicode_handling(self, temp_dir):
        """Test handling of Unicode characters."""
        handler = CSVHandler()
        
        commits = [
            Commit(
                sha="unicode123",
                author_name="François",
                author_email="françois@example.com",
                authored_date=datetime.now(timezone.utc),
                committer_name="李明",
                committer_email="liming@example.com",
                committed_date=datetime.now(timezone.utc),
                message="Fix: émojis 🐛 and spëcial çharacters",
                files_changed=["café.py"],
                additions=1,
                deletions=0,
            ),
        ]
        
        csv_path = temp_dir / "unicode_commits.csv"
        handler.export_commits(commits, csv_path)
        
        # Import back
        imported = handler.import_commits(csv_path)
        
        assert imported[0].author_name == "François"
        assert imported[0].committer_name == "李明"
        assert "émojis 🐛" in imported[0].message
        assert imported[0].files_changed[0] == "café.py"
        
    def test_empty_fields_handling(self, temp_dir):
        """Test handling of empty/null fields."""
        handler = CSVHandler()
        
        prs = [
            PullRequest(
                number=300,
                title="Minimal PR",
                state=PRState.MERGED,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                closed_at=None,  # Not closed
                merged_at=None,  # Not merged
                merge_commit_sha=None,
                commits=[],  # No commits
                author=None,  # No author
                labels=[],  # No labels
            ),
        ]
        
        csv_path = temp_dir / "minimal_prs.csv"
        handler.export_pull_requests(prs, csv_path)
        
        # Import back
        imported = handler.import_pull_requests(csv_path)
        
        pr = imported[0]
        assert pr.closed_at is None
        assert pr.merged_at is None
        assert pr.merge_commit_sha is None
        assert pr.commits == []
        assert pr.author is None
        assert pr.labels == []
        assert pr.is_hotfix is None  # No labels means None