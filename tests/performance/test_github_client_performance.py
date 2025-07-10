"""Performance tests for GitHub GraphQL client."""

import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.extractors.github_client import GitHubGraphQLClient


@pytest.mark.performance
@pytest.mark.requires_github
class TestGitHubGraphQLClientPerformance:
    """Performance tests using real GitHub API to prevent regressions."""
    
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
        # Using octocat/Hello-World - has thousands of PRs
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
        
    def test_fetch_with_max_results_performance(self, github_client):
        """Test that max_results parameter improves performance."""
        # Fetch without limit (baseline) - but cap at 500 to avoid timeout
        start_time = time.time()
        all_prs = github_client.fetch_pull_requests(state="CLOSED", max_results=500)
        baseline_time = time.time() - start_time
        baseline_count = len(all_prs)
        
        # Fetch with smaller max_results
        start_time = time.time()
        limited_prs = github_client.fetch_pull_requests(state="CLOSED", max_results=10)
        limited_time = time.time() - start_time
        
        assert len(limited_prs) == 10
        assert baseline_count <= 500  # Should respect max_results
        assert baseline_count > 100   # Should have fetched multiple pages
        # With max_results=10 should be significantly faster
        assert limited_time < baseline_time / 3
        # Should complete reasonably quickly
        assert limited_time < 10.0
        
        print(f"Performance improvement: {baseline_time:.2f}s -> {limited_time:.2f}s")
        print(f"Fetched {baseline_count} PRs vs {len(limited_prs)} PRs")
        
    def test_single_page_performance(self, github_client):
        """Test performance when results fit in a single page (<=100 items)."""
        # Request less than page size (100)
        start_time = time.time()
        prs = github_client.fetch_pull_requests(state="CLOSED", max_results=50)
        elapsed_time = time.time() - start_time
        
        assert len(prs) == 50
        # Single page query should complete within reasonable time
        assert elapsed_time < 15.0
        print(f"Single page (50 items) completed in {elapsed_time:.2f}s")
        
    def test_multi_page_performance(self, github_client):
        """Test performance when pagination is required (>100 items)."""
        # Test fetching exactly 150 items (requires 2 pages)
        start_time = time.time()
        prs = github_client.fetch_pull_requests(
            state="CLOSED",
            max_results=150
        )
        elapsed_time = time.time() - start_time
        
        assert len(prs) == 150
        # Should complete within reasonable time for 2 pages
        assert elapsed_time < 25.0
        
        # Test that max_results stops pagination early
        start_time = time.time()
        limited_prs = github_client.fetch_pull_requests(
            state="CLOSED",
            max_results=50  # Less than page size
        )
        limited_time = time.time() - start_time
        
        assert len(limited_prs) == 50
        # Should be much faster since we stop after first page
        assert limited_time < elapsed_time / 2
        
    def test_date_range_with_pagination(self, github_client):
        """Test performance of date filtering with pagination."""
        # Use date range that might span multiple pages
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=365)  # Last year
        
        start_time = time.time()
        prs = github_client.fetch_pull_requests(
            since=start_date,
            until=end_date,
            max_results=200  # May require multiple pages
        )
        elapsed_time = time.time() - start_time
        
        # Should complete within reasonable time
        assert elapsed_time < 20.0
        # Should respect max_results
        assert len(prs) <= 200
        # All PRs should be within date range
        for pr in prs:
            assert pr.created_at >= start_date
            assert pr.created_at <= end_date
            
    def test_release_fetch_performance(self, github_client):
        """Test performance of fetching releases."""
        start_time = time.time()
        releases = github_client.fetch_releases(max_results=10)
        elapsed_time = time.time() - start_time
        
        # Release queries should also be fast
        assert elapsed_time < 10.0
        assert len(releases) <= 10