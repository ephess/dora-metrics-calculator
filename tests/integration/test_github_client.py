"""Integration tests for GitHub GraphQL client."""

import os
from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.extractors.github_client import GitHubGraphQLClient
from dora_metrics.models import PRState


@pytest.mark.integration
@pytest.mark.requires_github
class TestGitHubGraphQLClientIntegration:
    """Integration tests using real GitHub API."""
    
    @pytest.fixture
    def github_token(self):
        """Get GitHub token from environment."""
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            pytest.skip("GITHUB_TOKEN environment variable not set")
        return token
        
    @pytest.fixture
    def test_repo(self):
        """Test repository details."""
        # Using a smaller public repository for faster testing
        return {
            "owner": "octocat",
            "repo": "Hello-World"
        }
        
    @pytest.fixture
    def github_client(self, github_token, test_repo):
        """Create a real GitHub client."""
        return GitHubGraphQLClient(
            token=github_token,
            owner=test_repo["owner"],
            repo=test_repo["repo"]
        )
        
    def test_fetch_pull_requests_real_api(self, github_client):
        """Test fetching real pull requests."""
        # Fetch only a few PRs for faster testing
        prs = github_client.fetch_pull_requests(max_results=5)
        
        # Should have some PRs
        assert len(prs) > 0
        assert len(prs) <= 5
        
        # Verify PR structure
        for pr in prs:
            assert pr.number > 0
            assert pr.title
            assert pr.state in [PRState.OPEN, PRState.CLOSED, PRState.MERGED]
            assert pr.created_at
            assert isinstance(pr.commits, list)
            assert isinstance(pr.labels, list)
            
    def test_fetch_releases_real_api(self, github_client):
        """Test fetching real releases."""
        # Fetch only a few releases for faster testing
        releases = github_client.fetch_releases(max_results=3)
        
        # Verify release structure if any exist
        for release in releases:
            assert release.tag_name
            assert release.created_at
            assert isinstance(release.is_prerelease, bool)
            
    def test_pagination_with_real_api(self, github_client):
        """Test pagination works correctly."""
        # For Hello-World repo, just verify we can fetch closed PRs with limit
        prs = github_client.fetch_pull_requests(state="CLOSED", max_results=10)
        assert len(prs) > 0
        assert len(prs) <= 10
            
    def test_date_filtering_with_real_api(self, github_client):
        """Test date filtering works correctly."""
        # Get PRs from a recent date range with limit
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=365)  # Last year
        
        prs = github_client.fetch_pull_requests(
            since=start_date, 
            until=end_date,
            max_results=5
        )
        
        # Verify all PRs are within date range
        for pr in prs:
            assert pr.created_at >= start_date
            assert pr.created_at <= end_date
            
    def test_rate_limit_info(self, github_client):
        """Test that rate limit info is properly handled."""
        # Make a small request
        prs = github_client.fetch_pull_requests(state="OPEN", max_results=2)
        
        # Should not raise any rate limit errors
        assert isinstance(prs, list)
        assert len(prs) <= 2