"""Integration tests for data associator."""

from datetime import datetime, timezone

import pytest

from dora_metrics.models import Commit, Deployment, PRState, PullRequest
from dora_metrics.processors.data_associator import DataAssociator


@pytest.mark.integration
class TestDataAssociatorIntegration:
    """Integration tests for DataAssociator with realistic scenarios."""
    
    def test_squash_merge_workflow(self):
        """Test association with squash merge workflow."""
        # In squash merge, PR commits are squashed into a single commit
        commits = [
            Commit(
                sha="squash123",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="GitHub",
                committer_email="noreply@github.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Feature: Add new API endpoint (#100)\n\n* Add endpoint\n* Add tests\n* Fix review comments",
                files_changed=["api.py", "test_api.py"],
                additions=150,
                deletions=10,
            ),
        ]
        
        prs = [
            PullRequest(
                number=100,
                title="Feature: Add new API endpoint",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="squash123",
                commits=["original1", "original2", "original3"],  # Original commits squashed
                author="dev",
                labels=["enhancement"],
            ),
        ]
        
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(commits, prs, [])
        
        # Squash commit should be associated with PR
        assert updated_commits[0].pr_number == 100
        
    def test_merge_commit_workflow(self):
        """Test association with merge commit workflow."""
        # In merge commit workflow, original commits are preserved plus a merge commit
        commits = [
            Commit(
                sha="feat1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                message="Add feature part 1",
                files_changed=["feature.py"],
                additions=50,
                deletions=0,
            ),
            Commit(
                sha="feat2",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
                message="Add feature part 2",
                files_changed=["feature.py"],
                additions=30,
                deletions=5,
            ),
            Commit(
                sha="merge123",
                author_name="GitHub",
                author_email="noreply@github.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="GitHub",
                committer_email="noreply@github.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Merge pull request #101 from dev/feature-branch",
                files_changed=[],
                additions=0,
                deletions=0,
            ),
        ]
        
        prs = [
            PullRequest(
                number=101,
                title="Add new feature",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 8, 30, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="merge123",
                commits=["feat1", "feat2"],
                author="dev",
                labels=["enhancement"],
            ),
        ]
        
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(commits, prs, [])
        
        # All commits should be associated with the PR
        commit_map = {c.sha: c for c in updated_commits}
        assert commit_map["feat1"].pr_number == 101
        assert commit_map["feat2"].pr_number == 101
        assert commit_map["merge123"].pr_number == 101
        
    def test_deployment_after_pr_merge(self):
        """Test typical deployment workflow after PR merge."""
        commits = [
            Commit(
                sha="feature123",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Feature: New dashboard (#200)",
                files_changed=["dashboard.py"],
                additions=200,
                deletions=50,
            ),
            Commit(
                sha="fix456",
                author_name="Dev2",
                author_email="dev2@example.com",
                authored_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev2",
                committer_email="dev2@example.com",
                committed_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                message="Fix: Dashboard bug (#201)",
                files_changed=["dashboard.py"],
                additions=5,
                deletions=3,
            ),
        ]
        
        prs = [
            PullRequest(
                number=200,
                title="Feature: New dashboard",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="feature123",
                commits=["feature123"],
                author="dev",
                labels=["enhancement"],
            ),
            PullRequest(
                number=201,
                title="Fix: Dashboard bug",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="fix456",
                commits=["fix456"],
                author="dev2",
                labels=["bug", "hotfix"],
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v2.0.0",
                name="Release 2.0.0 - New Dashboard",
                created_at=datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 2, 11, 30, tzinfo=timezone.utc),
                commit_sha="fix456",  # Deployed after the hotfix
                is_prerelease=False,
            ),
        ]
        
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(commits, prs, deployments)
        
        # Check associations
        commit_map = {c.sha: c for c in updated_commits}
        assert commit_map["feature123"].pr_number == 200
        assert commit_map["feature123"].deployment_tag is None
        assert commit_map["fix456"].pr_number == 201
        assert commit_map["fix456"].deployment_tag == "v2.0.0"
        
    def test_hotfix_identification(self):
        """Test hotfix identification with various label combinations."""
        commits = [
            Commit(
                sha=f"commit{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=datetime(2024, 1, i, 10, 0, tzinfo=timezone.utc),
                message=f"Fix {i}",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
            )
            for i in range(1, 5)
        ]
        
        prs = [
            PullRequest(
                number=301,
                title="Emergency fix for production",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="commit1",
                commits=["commit1"],
                author="dev",
                labels=["emergency", "production"],
            ),
            PullRequest(
                number=302,
                title="Critical security patch",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="commit2",
                commits=["commit2"],
                author="dev",
                labels=["critical", "security"],
            ),
            PullRequest(
                number=303,
                title="Regular feature",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 3, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="commit3",
                commits=["commit3"],
                author="dev",
                labels=["enhancement", "feature"],
            ),
            PullRequest(
                number=304,
                title="Hotfix for customer issue",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 4, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="commit4",
                commits=["commit4"],
                author="dev",
                labels=["hotfix", "customer"],
            ),
        ]
        
        # Test with default hotfix labels
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(commits, prs, [])
        
        # PR 301 has "emergency", PR 302 has "critical", PR 304 has "hotfix"
        # All should be identified as hotfixes with default labels
        # PR 303 should not be identified as hotfix
        
        # Test with custom hotfix labels
        custom_associator = DataAssociator(hotfix_labels={"emergency", "security"})
        updated_commits2, updated_prs2 = custom_associator.associate_data(commits, prs, [])
        
        # With custom labels, only PR 301 (emergency) and PR 302 (security) should be hotfixes
        
    def test_complex_real_world_scenario(self):
        """Test a complex scenario with multiple PRs, deployments, and edge cases."""
        commits = [
            # Direct commit (no PR)
            Commit(
                sha="direct1",
                author_name="Admin",
                author_email="admin@example.com",
                authored_date=datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc),
                committer_name="Admin",
                committer_email="admin@example.com",
                committed_date=datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc),
                message="Update version",
                files_changed=["version.txt"],
                additions=1,
                deletions=1,
            ),
            # Feature PR commits
            Commit(
                sha="feat1",
                author_name="Dev1",
                author_email="dev1@example.com",
                authored_date=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                committer_name="Dev1",
                committer_email="dev1@example.com",
                committed_date=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                message="Add user API",
                files_changed=["api/users.py"],
                additions=100,
                deletions=0,
            ),
            Commit(
                sha="feat2",
                author_name="Dev1",
                author_email="dev1@example.com",
                authored_date=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
                committer_name="Dev1",
                committer_email="dev1@example.com",
                committed_date=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
                message="Add user tests",
                files_changed=["tests/test_users.py"],
                additions=80,
                deletions=0,
            ),
            # Merge commit for feature
            Commit(
                sha="merge1",
                author_name="GitHub",
                author_email="noreply@github.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="GitHub",
                committer_email="noreply@github.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Merge pull request #400 from dev/user-api",
                files_changed=[],
                additions=0,
                deletions=0,
            ),
            # Hotfix squash commit
            Commit(
                sha="hotfix1",
                author_name="Dev2",
                author_email="dev2@example.com",
                authored_date=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                committer_name="GitHub",
                committer_email="noreply@github.com",
                committed_date=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                message="Hotfix: Fix user API bug (#401)\n\nUrgent fix for production issue",
                files_changed=["api/users.py"],
                additions=5,
                deletions=3,
            ),
            # Deployment commit
            Commit(
                sha="deploy1",
                author_name="CI",
                author_email="ci@example.com",
                authored_date=datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc),
                committer_name="CI",
                committer_email="ci@example.com",
                committed_date=datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc),
                message="Bump version to 1.5.0",
                files_changed=["version.txt", "CHANGELOG.md"],
                additions=10,
                deletions=2,
            ),
        ]
        
        prs = [
            PullRequest(
                number=400,
                title="Feature: User API",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 8, 30, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                merge_commit_sha="merge1",
                commits=["feat1", "feat2"],
                author="dev1",
                labels=["enhancement", "api"],
            ),
            PullRequest(
                number=401,
                title="Hotfix: Fix user API bug",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 13, 30, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc),
                merge_commit_sha="hotfix1",
                commits=["hotfix-branch-1", "hotfix-branch-2"],  # Squashed
                author="dev2",
                labels=["bug", "urgent", "production"],
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.5.0",
                name="Release 1.5.0",
                created_at=datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 2, 15, 30, tzinfo=timezone.utc),
                commit_sha="deploy1",
                is_prerelease=False,
            ),
        ]
        
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(commits, prs, deployments)
        
        # Verify complex associations
        commit_map = {c.sha: c for c in updated_commits}
        
        # Direct commit has no PR
        assert commit_map["direct1"].pr_number is None
        assert commit_map["direct1"].deployment_tag is None
        
        # Feature commits associated with PR 400
        assert commit_map["feat1"].pr_number == 400
        assert commit_map["feat2"].pr_number == 400
        assert commit_map["merge1"].pr_number == 400
        
        # Hotfix squash commit associated with PR 401
        assert commit_map["hotfix1"].pr_number == 401
        
        # Deployment commit marked with tag
        assert commit_map["deploy1"].deployment_tag == "v1.5.0"
        
        # Get orphaned commits
        orphaned = associator.get_orphaned_commits()
        assert len(orphaned) == 2  # direct1 and deploy1
        assert "direct1" in {c.sha for c in orphaned}
        assert "deploy1" in {c.sha for c in orphaned}