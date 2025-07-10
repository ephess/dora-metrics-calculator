"""Unit tests for GitHub GraphQL client."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from dora_metrics.extractors.github_client import GitHubGraphQLClient
from dora_metrics.models import Deployment, PRState, PullRequest


@pytest.mark.unit
class TestGitHubGraphQLClient:
    """Test GitHub GraphQL client with mocked responses."""
    
    @pytest.fixture
    def mock_gql_client(self):
        """Create a mock GQL client."""
        with patch("dora_metrics.extractors.github_client.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client
            
    @pytest.fixture
    def github_client(self, mock_gql_client):
        """Create a GitHubGraphQLClient instance with mocked GQL client."""
        client = GitHubGraphQLClient(
            token="fake-token",
            owner="test-owner", 
            repo="test-repo"
        )
        client.client = mock_gql_client
        return client
        
    def test_init(self):
        """Test client initialization."""
        with patch("dora_metrics.extractors.github_client.Client"):
            client = GitHubGraphQLClient(
                token="test-token",
                owner="test-owner",
                repo="test-repo"
            )
            assert client.token == "test-token"
            assert client.owner == "test-owner"
            assert client.repo == "test-repo"
            
    def test_fetch_pull_requests_single_page(self, github_client, mock_gql_client):
        """Test fetching PRs with single page of results."""
        # Mock GraphQL response
        mock_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None
                    },
                    "nodes": [
                        {
                            "number": 123,
                            "title": "Add new feature",
                            "state": "OPEN",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "updatedAt": "2024-01-15T11:00:00Z",
                            "closedAt": None,
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": {"login": "testuser"},
                            "commits": {
                                "nodes": [
                                    {"commit": {"oid": "abc123"}},
                                    {"commit": {"oid": "def456"}}
                                ]
                            },
                            "labels": {
                                "nodes": [
                                    {"name": "enhancement"},
                                    {"name": "feature"}
                                ]
                            }
                        }
                    ]
                }
            },
            "rateLimit": {
                "remaining": 4999,
                "resetAt": "2024-01-15T12:00:00Z"
            }
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        # Fetch PRs
        prs = github_client.fetch_pull_requests()
        
        assert len(prs) == 1
        assert prs[0].number == 123
        assert prs[0].title == "Add new feature"
        assert prs[0].state == PRState.OPEN
        assert prs[0].author == "testuser"
        assert len(prs[0].commits) == 2
        assert "abc123" in prs[0].commits
        assert len(prs[0].labels) == 2
        assert "enhancement" in prs[0].labels
        
    def test_fetch_pull_requests_with_pagination(self, github_client, mock_gql_client):
        """Test fetching PRs with pagination."""
        # First page response
        page1_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "cursor1"
                    },
                    "nodes": [
                        {
                            "number": 1,
                            "title": "PR 1",
                            "state": "MERGED",
                            "createdAt": "2024-01-01T10:00:00Z",
                            "updatedAt": "2024-01-01T11:00:00Z",
                            "closedAt": "2024-01-01T11:00:00Z",
                            "mergedAt": "2024-01-01T11:00:00Z",
                            "mergeCommit": {"oid": "merge1"},
                            "author": {"login": "user1"},
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        }
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        # Second page response
        page2_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None
                    },
                    "nodes": [
                        {
                            "number": 2,
                            "title": "PR 2",
                            "state": "CLOSED",
                            "createdAt": "2024-01-02T10:00:00Z",
                            "updatedAt": "2024-01-02T11:00:00Z",
                            "closedAt": "2024-01-02T11:00:00Z",
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": {"login": "user2"},
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        }
                    ]
                }
            },
            "rateLimit": {"remaining": 4998, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.side_effect = [page1_response, page2_response]
        
        # Fetch PRs
        prs = github_client.fetch_pull_requests()
        
        assert len(prs) == 2
        assert prs[0].number == 1
        assert prs[0].state == PRState.MERGED
        assert prs[1].number == 2
        assert prs[1].state == PRState.CLOSED
        
    def test_fetch_pull_requests_with_date_filter(self, github_client, mock_gql_client):
        """Test fetching PRs with date filtering."""
        mock_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        {
                            "number": 1,
                            "title": "Old PR",
                            "state": "CLOSED",
                            "createdAt": "2023-12-01T10:00:00Z",
                            "updatedAt": "2023-12-01T11:00:00Z",
                            "closedAt": "2023-12-01T11:00:00Z",
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        },
                        {
                            "number": 2,
                            "title": "Recent PR",
                            "state": "OPEN",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "updatedAt": "2024-01-15T11:00:00Z",
                            "closedAt": None,
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        }
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        # Fetch PRs created after 2024-01-01
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        prs = github_client.fetch_pull_requests(since=since)
        
        assert len(prs) == 1
        assert prs[0].number == 2
        
    def test_fetch_releases_single_page(self, github_client, mock_gql_client):
        """Test fetching releases with single page of results."""
        mock_response = {
            "repository": {
                "releases": {
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None
                    },
                    "nodes": [
                        {
                            "tagName": "v1.0.0",
                            "name": "Version 1.0.0",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "publishedAt": "2024-01-15T10:30:00Z",
                            "isPrerelease": False,
                            "tagCommit": {"oid": "abc123"}
                        }
                    ]
                }
            },
            "rateLimit": {
                "remaining": 4999,
                "resetAt": "2024-01-15T12:00:00Z"
            }
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        # Fetch releases
        releases = github_client.fetch_releases()
        
        assert len(releases) == 1
        assert releases[0].tag_name == "v1.0.0"
        assert releases[0].name == "Version 1.0.0"
        assert releases[0].commit_sha == "abc123"
        assert not releases[0].is_prerelease
        
    def test_rate_limit_handling(self, github_client, mock_gql_client):
        """Test rate limit handling with retries."""
        # First call fails with rate limit
        rate_limit_error = Exception("API rate limit exceeded")
        
        # Second call succeeds
        success_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": []
                }
            },
            "rateLimit": {"remaining": 5000, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.side_effect = [rate_limit_error, success_response]
        
        # Should retry and succeed
        with patch("time.sleep"):  # Mock sleep to speed up test
            prs = github_client.fetch_pull_requests()
            
        assert len(prs) == 0
        assert mock_gql_client.execute.call_count == 2
        
    def test_label_fetching(self, github_client, mock_gql_client):
        """Test PR label fetching."""
        mock_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        {
                            "number": 1,
                            "title": "Critical bug fix",
                            "state": "MERGED",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "updatedAt": "2024-01-15T11:00:00Z",
                            "closedAt": "2024-01-15T11:00:00Z",
                            "mergedAt": "2024-01-15T11:00:00Z",
                            "mergeCommit": {"oid": "hotfix1"},
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": [{"name": "hotfix"}, {"name": "critical"}]}
                        },
                        {
                            "number": 2,
                            "title": "Add new feature",
                            "state": "MERGED",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "updatedAt": "2024-01-15T11:00:00Z",
                            "closedAt": "2024-01-15T11:00:00Z",
                            "mergedAt": "2024-01-15T11:00:00Z",
                            "mergeCommit": {"oid": "feature1"},
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": [{"name": "enhancement"}]}
                        }
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        prs = github_client.fetch_pull_requests()
        
        assert "hotfix" in prs[0].labels
        assert "critical" in prs[0].labels
        assert "enhancement" in prs[1].labels
        
    def test_max_results_limit(self, github_client, mock_gql_client):
        """Test max_results parameter limits results."""
        # Mock response with many PRs
        mock_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                    "nodes": [
                        {
                            "number": i,
                            "title": f"PR {i}",
                            "state": "OPEN",
                            "createdAt": f"2024-01-{i:02d}T10:00:00Z",
                            "updatedAt": f"2024-01-{i:02d}T11:00:00Z",
                            "closedAt": None,
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": {"login": f"user{i}"},
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        } for i in range(1, 6)  # 5 PRs
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        # Request only 3 PRs
        prs = github_client.fetch_pull_requests(max_results=3)
        
        assert len(prs) == 3
        assert prs[0].number == 1
        assert prs[1].number == 2
        assert prs[2].number == 3
        
    def test_max_results_with_pagination(self, github_client, mock_gql_client):
        """Test max_results works across multiple pages."""
        # First page
        page1 = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                    "nodes": [
                        {
                            "number": i,
                            "title": f"PR {i}",
                            "state": "OPEN",
                            "createdAt": "2024-01-01T10:00:00Z",
                            "updatedAt": "2024-01-01T11:00:00Z",
                            "closedAt": None,
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        } for i in range(1, 4)  # 3 PRs
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        # Second page (should not be requested)
        page2 = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        {
                            "number": i,
                            "title": f"PR {i}",
                            "state": "OPEN",
                            "createdAt": "2024-01-01T10:00:00Z",
                            "updatedAt": "2024-01-01T11:00:00Z",
                            "closedAt": None,
                            "mergedAt": None,
                            "mergeCommit": None,
                            "author": None,
                            "commits": {"nodes": []},
                            "labels": {"nodes": []}
                        } for i in range(4, 7)  # 3 more PRs
                    ]
                }
            },
            "rateLimit": {"remaining": 4998, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.side_effect = [page1, page2]
        
        # Request only 2 PRs - should stop after first page
        prs = github_client.fetch_pull_requests(max_results=2)
        
        assert len(prs) == 2
        assert prs[0].number == 1
        assert prs[1].number == 2
        # Should only call execute once since we hit the limit
        assert mock_gql_client.execute.call_count == 1
        
    def test_prerelease_handling(self, github_client, mock_gql_client):
        """Test prerelease handling."""
        mock_response = {
            "repository": {
                "releases": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        {
                            "tagName": "v1.0.0-beta.1",
                            "name": "Beta Release",
                            "createdAt": "2024-01-15T10:00:00Z",
                            "publishedAt": "2024-01-15T10:30:00Z",
                            "isPrerelease": True,
                            "tagCommit": {"oid": "beta123"}
                        },
                        {
                            "tagName": "v1.0.0",
                            "name": "Stable Release",
                            "createdAt": "2024-01-16T10:00:00Z",
                            "publishedAt": "2024-01-16T10:30:00Z",
                            "isPrerelease": False,
                            "tagCommit": {"oid": "stable123"}
                        }
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        releases = github_client.fetch_releases()
        
        assert len(releases) == 2
        assert releases[0].is_prerelease
        assert not releases[1].is_prerelease
        
    def test_rate_limit_wait_handling(self, github_client, mock_gql_client):
        """Test rate limit handling when we need to wait."""
        # Response with very low rate limit
        low_rate_response = {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": []
                }
            },
            "rateLimit": {
                "remaining": 5,  # Very low
                "resetAt": (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()
            }
        }
        
        mock_gql_client.execute.return_value = low_rate_response
        
        # Should handle low rate limit gracefully
        with patch("time.sleep") as mock_sleep:
            prs = github_client.fetch_pull_requests()
            
        assert len(prs) == 0
        # Should have waited due to low rate limit
        mock_sleep.assert_called()
        
    def test_max_retries_exceeded(self, github_client, mock_gql_client):
        """Test that max retries are enforced."""
        # Always fail with non-rate-limit error
        mock_gql_client.execute.side_effect = Exception("Network error")
        
        # Should raise the original exception after max retries
        with patch("time.sleep"):  # Mock sleep to speed up test
            with pytest.raises(Exception, match="Network error"):
                github_client.fetch_pull_requests()
                
    def test_fetch_releases_max_results(self, github_client, mock_gql_client):
        """Test max_results for releases."""
        mock_response = {
            "repository": {
                "releases": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                    "nodes": [
                        {
                            "tagName": f"v1.0.{i}",
                            "name": f"Release {i}",
                            "createdAt": f"2024-01-{i+1:02d}T10:00:00Z",
                            "publishedAt": f"2024-01-{i+1:02d}T10:30:00Z",
                            "isPrerelease": False,
                            "tagCommit": {"oid": f"sha{i}"}
                        } for i in range(5)
                    ]
                }
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2024-01-15T12:00:00Z"}
        }
        
        mock_gql_client.execute.return_value = mock_response
        
        # Request only 2 releases
        releases = github_client.fetch_releases(max_results=2)
        
        assert len(releases) == 2
        assert releases[0].tag_name == "v1.0.0"
        assert releases[1].tag_name == "v1.0.1"