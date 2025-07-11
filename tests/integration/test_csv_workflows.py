"""Integration tests for CSV export/import workflows."""

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dora_metrics.models import Commit, Deployment, PRState, PullRequest
from dora_metrics.storage.csv_handler import CSVHandler


@pytest.mark.integration
class TestCSVWorkflows:
    """Test complete CSV workflows."""
    
    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for CSV files."""
        return tmp_path
        
    def test_full_export_import_cycle(self, temp_dir):
        """Test exporting data and re-importing preserves information."""
        handler = CSVHandler()
        
        # Create test data
        commits = [
            Commit(
                sha=f"commit{i}",
                author_name=f"Dev {i}",
                author_email=f"dev{i}@example.com",
                authored_date=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                committer_name=f"Dev {i}",
                committer_email=f"dev{i}@example.com",
                committed_date=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                message=f"Commit {i}",
                files_changed=[f"file{i}.py"],
                additions=i * 10,
                deletions=i * 5,
                pr_number=100 + i if i % 2 == 0 else None,
                deployment_tag="v1.0.0" if i == 3 else None,
            )
            for i in range(1, 5)
        ]
        
        prs = [
            PullRequest(
                number=100 + i,
                title=f"PR {i}",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, i, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha=f"commit{i * 2}",
                commits=[f"commit{i * 2}"],
                author=f"dev{i}",
                labels=["urgent"] if i == 1 else ["enhancement"],
            )
            for i in range(1, 3)
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Release 1.0.0",
                created_at=datetime(2024, 1, 3, 11, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 3, 11, 30, tzinfo=timezone.utc),
                commit_sha="commit3",
                is_prerelease=False,
            ),
        ]
        
        # Export
        handler.export_commits(commits, temp_dir / "commits.csv")
        handler.export_pull_requests(prs, temp_dir / "prs.csv")
        handler.export_deployments(deployments, temp_dir / "deployments.csv")
        
        # Re-import
        imported_commits = handler.import_commits(temp_dir / "commits.csv")
        imported_prs = handler.import_pull_requests(temp_dir / "prs.csv")
        imported_deployments = handler.import_deployments(temp_dir / "deployments.csv")
        
        # Verify data integrity
        assert len(imported_commits) == 4
        assert len(imported_prs) == 2
        assert len(imported_deployments) == 1
        
        # Check specific values
        assert imported_commits[2].deployment_tag == "v1.0.0"
        assert imported_prs[0].is_hotfix is True  # urgent label
        assert imported_prs[1].is_hotfix is False  # enhancement label
        
    def test_manual_annotation_workflow(self, temp_dir):
        """Test workflow with manual annotations added."""
        handler = CSVHandler()
        
        # Initial data - commits without deployment info
        commits = [
            Commit(
                sha="prod001",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Production deployment",
                files_changed=["app.py"],
                additions=50,
                deletions=10,
            ),
            Commit(
                sha="hotfix001",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                message="Emergency fix",
                files_changed=["auth.py"],
                additions=5,
                deletions=3,
            ),
        ]
        
        # Export
        csv_path = temp_dir / "commits.csv"
        handler.export_commits(commits, csv_path)
        
        # Simulate human editing the CSV
        import csv
        rows = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["sha"] == "prod001":
                    row["is_manual_deployment"] = "true"
                    row["manual_deployment_timestamp"] = "2024-01-01T18:00:00+00:00"
                    row["notes"] = "Regular release"
                elif row["sha"] == "hotfix001":
                    row["is_manual_deployment"] = "true"
                    row["manual_deployment_timestamp"] = "2024-01-02T15:30:00+00:00"
                    row["manual_deployment_failed"] = "true"
                    row["notes"] = "Rolled back due to errors"
                rows.append(row)
                
        # Write back
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.COMMIT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            
        # Re-import
        annotated_commits = handler.import_commits(csv_path)
        
        # Verify annotations
        assert annotated_commits[0].is_manual_deployment is True
        assert annotated_commits[0].manual_deployment_timestamp == datetime(2024, 1, 1, 18, 0, tzinfo=timezone.utc)
        assert annotated_commits[0].manual_deployment_failed is None  # Not set
        assert annotated_commits[0].notes == "Regular release"
        
        assert annotated_commits[1].is_manual_deployment is True
        assert annotated_commits[1].manual_deployment_timestamp == datetime(2024, 1, 2, 15, 30, tzinfo=timezone.utc)
        assert annotated_commits[1].manual_deployment_failed is True
        assert annotated_commits[1].notes == "Rolled back due to errors"
        
    def test_excel_compatibility(self, temp_dir):
        """Test handling of Excel-edited CSV files."""
        # Simulate Excel-style CSV with extra commas and quotes
        csv_path = temp_dir / "excel_commits.csv"
        
        with open(csv_path, "w", encoding="utf-8-sig") as f:
            # Excel sometimes adds BOM and uses different quoting
            f.write('\ufeff')  # BOM
            f.write('"sha","author_name","author_email","authored_date","committer_name",')
            f.write('"committer_email","committed_date","message","files_changed",')
            f.write('"additions","deletions","pr_number","deployment_tag",')
            f.write('"is_manual_deployment","manual_deployment_timestamp","manual_deployment_failed","notes"\n')
            f.write('"excel123","Dev Name","dev@example.com","2024-01-01T10:00:00+00:00",')
            f.write('"Dev Name","dev@example.com","2024-01-01T10:00:00+00:00",')
            f.write('"Fix: Issue with ""quotes""","file1.py|file2.py","10","5","123","",')
            f.write('"TRUE","2024-01-01T15:00:00+00:00","FALSE","Notes with, comma"\n')
            
        handler = CSVHandler()
        commits = handler.import_commits(csv_path)
        
        assert len(commits) == 1
        commit = commits[0]
        assert commit.sha == "excel123"
        assert 'quotes' in commit.message
        assert commit.is_manual_deployment is True
        assert commit.manual_deployment_failed is False
        assert "comma" in commit.notes
        
    def test_incremental_updates(self, temp_dir):
        """Test workflow for incremental updates with preserved annotations."""
        handler = CSVHandler()
        
        # Initial export
        initial_commits = [
            Commit(
                sha="old001",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Old commit",
                files_changed=["old.py"],
                additions=10,
                deletions=5,
            ),
        ]
        
        csv_path = temp_dir / "commits.csv"
        handler.export_commits(initial_commits, csv_path)
        
        # Add annotation
        rows = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["is_manual_deployment"] = "true"
                row["notes"] = "Historical deployment"
                rows.append(row)
                
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.COMMIT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            
        # Import to get annotations
        annotated = handler.import_commits(csv_path)
        
        # Add new commits (simulating incremental update)
        all_commits = annotated + [
            Commit(
                sha="new001",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                message="New commit",
                files_changed=["new.py"],
                additions=20,
                deletions=0,
            ),
        ]
        
        # Re-export (should preserve annotations)
        handler.export_commits(all_commits, csv_path)
        
        # Re-import and verify
        final = handler.import_commits(csv_path)
        
        assert len(final) == 2
        assert final[0].sha == "old001"
        assert final[0].is_manual_deployment is True
        assert final[0].notes == "Historical deployment"
        assert final[1].sha == "new001"
        assert final[1].is_manual_deployment is None
        
    def test_pr_hotfix_override(self, temp_dir):
        """Test manual override of hotfix detection."""
        handler = CSVHandler()
        
        # PR without hotfix labels
        prs = [
            PullRequest(
                number=500,
                title="Regular feature",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
                merge_commit_sha="feat500",
                commits=["feat500"],
                author="dev",
                labels=["enhancement", "documentation"],
            ),
        ]
        
        csv_path = temp_dir / "prs.csv"
        handler.export_pull_requests(prs, csv_path)
        
        # Verify auto-detection said false
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["is_hotfix"] == "false"
            
        # Human edits to mark as hotfix
        rows = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["is_hotfix"] = "true"
                row["notes"] = "Was actually an emergency fix"
                rows.append(row)
                
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.PR_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            
        # Re-import
        imported = handler.import_pull_requests(csv_path)
        
        assert imported[0].is_hotfix is True
        assert imported[0].notes == "Was actually an emergency fix"
        
    def test_deployment_failure_tracking(self, temp_dir):
        """Test tracking deployment failures and resolutions."""
        handler = CSVHandler()
        
        deployments = [
            Deployment(
                tag_name="v2.0.0",
                name="Major Release",
                created_at=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
                commit_sha="major200",
                is_prerelease=False,
            ),
            Deployment(
                tag_name="v2.0.1",
                name="Hotfix Release",
                created_at=datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 15, 14, 15, tzinfo=timezone.utc),
                commit_sha="fix201",
                is_prerelease=False,
            ),
        ]
        
        csv_path = temp_dir / "deployments.csv"
        handler.export_deployments(deployments, csv_path)
        
        # Annotate with failure info
        rows = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["tag_name"] == "v2.0.0":
                    row["deployment_failed"] = "true"
                    row["failure_resolved_at"] = "2024-01-15T14:15:00+00:00"
                    row["notes"] = "Database migration failed, fixed in v2.0.1"
                rows.append(row)
                
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSVHandler.DEPLOYMENT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            
        # Re-import
        imported = handler.import_deployments(csv_path)
        
        assert imported[0].deployment_failed is True
        assert imported[0].failure_resolved_at == datetime(2024, 1, 15, 14, 15, tzinfo=timezone.utc)
        assert "migration failed" in imported[0].notes
        assert imported[1].deployment_failed is None  # Not failed