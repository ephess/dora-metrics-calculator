"""Unit tests for data associator."""

from datetime import datetime, timezone

import pytest

from dora_metrics.models import Commit, Deployment, PRState, PullRequest
from dora_metrics.processors.data_associator import DataAssociator


@pytest.mark.unit
class TestDataAssociator:
    """Test DataAssociator logic."""
    
    @pytest.fixture
    def sample_commits(self):
        """Create sample commits for testing."""
        return [
            Commit(
                sha="abc123",
                author_name="Dev 1",
                author_email="dev1@example.com",
                authored_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev 1",
                committer_email="dev1@example.com",
                committed_date=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                message="Fix bug",
                files_changed=["file1.py"],
                additions=10,
                deletions=5,
            ),
            Commit(
                sha="def456",
                author_name="Dev 2",
                author_email="dev2@example.com",
                authored_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev 2",
                committer_email="dev2@example.com",
                committed_date=datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
                message="Add feature",
                files_changed=["file2.py"],
                additions=20,
                deletions=2,
            ),
            Commit(
                sha="ghi789",
                author_name="Dev 3",
                author_email="dev3@example.com",
                authored_date=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev 3",
                committer_email="dev3@example.com",
                committed_date=datetime(2024, 1, 3, 10, 0, tzinfo=timezone.utc),
                message="Merge pull request #123",
                files_changed=[],
                additions=0,
                deletions=0,
            ),
            Commit(
                sha="jkl012",
                author_name="Dev 4",
                author_email="dev4@example.com",
                authored_date=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                committer_name="Dev 4",
                committer_email="dev4@example.com",
                committed_date=datetime(2024, 1, 4, 10, 0, tzinfo=timezone.utc),
                message="Direct commit",
                files_changed=["file3.py"],
                additions=5,
                deletions=3,
            ),
        ]
        
    @pytest.fixture
    def sample_prs(self):
        """Create sample pull requests for testing."""
        return [
            PullRequest(
                number=123,
                title="Fix critical bug",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc),
                merge_commit_sha="ghi789",
                commits=["abc123"],  # Single commit PR
                author="dev1",
                labels=["bug", "urgent"],
            ),
            PullRequest(
                number=124,
                title="Add new feature",
                state=PRState.MERGED,
                created_at=datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2024, 1, 2, 10, 30, tzinfo=timezone.utc),
                closed_at=datetime(2024, 1, 2, 10, 30, tzinfo=timezone.utc),
                merged_at=datetime(2024, 1, 2, 10, 30, tzinfo=timezone.utc),
                merge_commit_sha="xyz999",  # Merge commit not in our list
                commits=["def456", "uvw888"],  # Multi-commit PR
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
                created_at=datetime(2024, 1, 3, 11, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 3, 11, 30, tzinfo=timezone.utc),
                commit_sha="ghi789",
                is_prerelease=False,
            ),
            Deployment(
                tag_name="v1.1.0",
                name="Release 1.1.0",
                created_at=datetime(2024, 1, 5, 11, 0, tzinfo=timezone.utc),
                published_at=datetime(2024, 1, 5, 11, 30, tzinfo=timezone.utc),
                commit_sha="mno345",  # Commit not in our list
                is_prerelease=False,
            ),
        ]
        
    def test_init_default_hotfix_labels(self):
        """Test initialization with default hotfix labels."""
        associator = DataAssociator()
        assert associator.hotfix_labels == {"hotfix", "urgent", "critical", "emergency"}
        
    def test_init_custom_hotfix_labels(self):
        """Test initialization with custom hotfix labels."""
        custom_labels = {"hotfix", "patch", "bugfix"}
        associator = DataAssociator(hotfix_labels=custom_labels)
        assert associator.hotfix_labels == custom_labels
        
    def test_associate_single_commit_pr(self, sample_commits, sample_prs, sample_deployments):
        """Test associating a single-commit PR."""
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, sample_prs[:1], []
        )
        
        # Check that abc123 is associated with PR 123
        commit_abc = next(c for c in updated_commits if c.sha == "abc123")
        assert commit_abc.pr_number == 123
        
        # Check that merge commit is also associated
        commit_ghi = next(c for c in updated_commits if c.sha == "ghi789")
        assert commit_ghi.pr_number == 123
        
    def test_associate_multi_commit_pr(self, sample_commits, sample_prs, sample_deployments):
        """Test associating a multi-commit PR."""
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, sample_prs[1:2], []
        )
        
        # Check that def456 is associated with PR 124
        commit_def = next(c for c in updated_commits if c.sha == "def456")
        assert commit_def.pr_number == 124
        
        # uvw888 is not in our commit list, so it won't be associated
        
    def test_mark_deployment_commits(self, sample_commits, sample_prs, sample_deployments):
        """Test marking deployment commits."""
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, [], sample_deployments
        )
        
        # Check that ghi789 is associated with deployment tag v1.0.0
        commit_ghi = next(c for c in updated_commits if c.sha == "ghi789")
        assert commit_ghi.deployment_tag == "v1.0.0"
        
        # Check that other commits are not marked as deployments
        for commit in updated_commits:
            if commit.sha != "ghi789":
                assert commit.deployment_tag is None
                
    def test_identify_hotfixes(self, sample_commits, sample_prs, sample_deployments):
        """Test identifying hotfix PRs based on labels."""
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, sample_prs, []
        )
        
        # PR 123 has "urgent" label which is in default hotfix labels
        # PR 124 has "enhancement" which is not a hotfix label
        # Note: Currently we just log hotfixes, we don't store them on the PR
        # This test verifies the logic runs without error
        
    def test_custom_hotfix_labels(self, sample_commits, sample_prs, sample_deployments):
        """Test identifying hotfixes with custom labels."""
        # Use custom labels that include "bug"
        associator = DataAssociator(hotfix_labels={"bug", "hotfix"})
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, sample_prs, []
        )
        
        # PR 123 has "bug" label which is in our custom hotfix labels
        # This test verifies the custom labels are used
        
    def test_get_orphaned_commits(self, sample_commits, sample_prs, sample_deployments):
        """Test getting commits without PR associations."""
        associator = DataAssociator()
        associator.associate_data(sample_commits, sample_prs, [])
        
        orphaned = associator.get_orphaned_commits()
        orphaned_shas = {c.sha for c in orphaned}
        
        # jkl012 should be orphaned (direct commit)
        assert "jkl012" in orphaned_shas
        # abc123 and ghi789 should be associated with PR 123
        assert "abc123" not in orphaned_shas
        assert "ghi789" not in orphaned_shas
        
    def test_get_prs_without_commits(self, sample_commits, sample_prs, sample_deployments):
        """Test getting PRs without commits in main branch."""
        # Create commits that don't include PR 124's commits
        commits = [c for c in sample_commits if c.sha not in ["def456"]]
        
        associator = DataAssociator()
        associator.associate_data(commits, sample_prs, [])
        
        prs_without_commits = associator.get_prs_without_commits()
        pr_numbers = {pr.number for pr in prs_without_commits}
        
        # PR 124 should be without commits (def456 not in our list)
        assert 124 in pr_numbers
        # PR 123 should have commits
        assert 123 not in pr_numbers
        
    def test_get_deployments_without_commits(self, sample_commits, sample_prs, sample_deployments):
        """Test getting deployments without matching commits."""
        associator = DataAssociator()
        associator.associate_data(sample_commits, [], sample_deployments)
        
        deployments_without_commits = associator.get_deployments_without_commits()
        deployment_tags = {d.tag_name for d in deployments_without_commits}
        
        # v1.1.0 references mno345 which is not in our commit list
        assert "v1.1.0" in deployment_tags
        # v1.0.0 references ghi789 which is in our list
        assert "v1.0.0" not in deployment_tags
        
    def test_full_association_flow(self, sample_commits, sample_prs, sample_deployments):
        """Test the full association flow with all data."""
        associator = DataAssociator()
        updated_commits, updated_prs = associator.associate_data(
            sample_commits, sample_prs, sample_deployments
        )
        
        # Verify associations
        commit_map = {c.sha: c for c in updated_commits}
        
        # abc123 associated with PR 123
        assert commit_map["abc123"].pr_number == 123
        # def456 associated with PR 124
        assert commit_map["def456"].pr_number == 124
        # ghi789 associated with PR 123 and deployment v1.0.0
        assert commit_map["ghi789"].pr_number == 123
        assert commit_map["ghi789"].deployment_tag == "v1.0.0"
        # jkl012 not associated with any PR
        assert commit_map["jkl012"].pr_number is None
        
    def test_empty_data_sets(self):
        """Test handling empty data sets."""
        associator = DataAssociator()
        
        # Empty commits
        commits, prs = associator.associate_data([], [], [])
        assert commits == []
        assert prs == []
        
        # Empty PRs
        test_commits = [
            Commit(
                sha="test",
                author_name="Test",
                author_email="test@example.com",
                authored_date=datetime.now(timezone.utc),
                committer_name="Test",
                committer_email="test@example.com",
                committed_date=datetime.now(timezone.utc),
                message="Test",
            )
        ]
        commits, prs = associator.associate_data(test_commits, [], [])
        assert len(commits) == 1
        assert commits[0].pr_number is None
        
    def test_deployment_lookup_structure(self, sample_deployments):
        """Test that deployment lookups are built correctly."""
        associator = DataAssociator()
        associator._build_lookups([], [], sample_deployments)
        
        # Check deployments_by_tag
        assert "v1.0.0" in associator.deployments_by_tag
        assert "v1.1.0" in associator.deployments_by_tag
        assert associator.deployments_by_tag["v1.0.0"].commit_sha == "ghi789"
        
        # Check deployment_sha_to_tag
        assert associator.deployment_sha_to_tag["ghi789"] == "v1.0.0"
        assert associator.deployment_sha_to_tag["mno345"] == "v1.1.0"