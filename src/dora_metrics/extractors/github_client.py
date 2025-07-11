"""GitHub GraphQL client for fetching PR and release data."""

import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from ..logging import get_logger
from ..models import Deployment, PRState, PullRequest

logger = get_logger(__name__)


class GitHubGraphQLClient:
    """Client for interacting with GitHub GraphQL API."""
    
    def __init__(self, token: str, owner: str, repo: str):
        """
        Initialize GitHub GraphQL client.
        
        Args:
            token: GitHub personal access token
            owner: Repository owner (organization or user)
            repo: Repository name
        """
        self.token = token
        self.owner = owner
        self.repo = repo
        
        # Set up GraphQL client
        transport = RequestsHTTPTransport(
            url="https://api.github.com/graphql",
            headers={"Authorization": f"Bearer {token}"},
            retries=3,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        
    def fetch_pull_requests(
        self, 
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        state: Optional[str] = None,
        max_results: Optional[int] = None,
        callback: Optional[callable] = None,
    ) -> List[PullRequest]:
        """
        Fetch pull requests from GitHub.
        
        Args:
            since: Start date for filtering PRs
            until: End date for filtering PRs
            state: PR state filter (OPEN, CLOSED, MERGED)
            max_results: Maximum number of PRs to return
            callback: Optional callback function(pr_batch, total_fetched, estimated_total)
            
        Returns:
            List of PullRequest objects
        """
        logger.info(f"Fetching pull requests for {self.owner}/{self.repo}")
        
        # Build GraphQL query
        query = gql("""
            query($owner: String!, $repo: String!, $cursor: String, $states: [PullRequestState!]) {
                repository(owner: $owner, name: $repo) {
                    pullRequests(first: 100, after: $cursor, states: $states, orderBy: {field: CREATED_AT, direction: DESC}) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            number
                            title
                            state
                            createdAt
                            updatedAt
                            closedAt
                            mergedAt
                            mergeCommit {
                                oid
                            }
                            author {
                                login
                            }
                            commits(first: 100) {
                                nodes {
                                    commit {
                                        oid
                                    }
                                }
                            }
                            labels(first: 20) {
                                nodes {
                                    name
                                }
                            }
                        }
                    }
                }
                rateLimit {
                    remaining
                    resetAt
                }
            }
        """)
        
        # Convert state filter
        states = None
        if state:
            states = [state]
        
        # Fetch all pages
        all_prs = []
        cursor = None
        page_count = 0
        
        while True:
            variables = {
                "owner": self.owner,
                "repo": self.repo,
                "cursor": cursor,
                "states": states,
            }
            
            # Execute query with rate limit handling
            result = self._execute_with_rate_limit(query, variables)
            
            # Process results
            pr_nodes = result["repository"]["pullRequests"]["nodes"]
            page_prs = []
            
            for pr_data in pr_nodes:
                pr = self._parse_pull_request(pr_data)
                
                # Apply date filtering
                if since and pr.created_at < since:
                    continue
                if until and pr.created_at > until:
                    continue
                    
                all_prs.append(pr)
                page_prs.append(pr)
                
                # Check if we've hit the max results limit
                if max_results and len(all_prs) >= max_results:
                    logger.info(f"Reached max_results limit of {max_results}")
                    if callback and page_prs:
                        callback(page_prs, len(all_prs), max_results)
                    return all_prs[:max_results]
            
            # Call callback with this page's results
            page_count += 1
            if callback and page_prs:
                # Estimate total based on pages so far (rough estimate)
                estimated_total = len(all_prs) * 2 if page_count == 1 else None
                callback(page_prs, len(all_prs), estimated_total)
            
            # Check for more pages
            page_info = result["repository"]["pullRequests"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
                
            cursor = page_info["endCursor"]
            
        logger.info(f"Fetched {len(all_prs)} pull requests")
        return all_prs
    
    def fetch_releases(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        max_results: Optional[int] = None,
    ) -> List[Deployment]:
        """
        Fetch releases from GitHub.
        
        Args:
            since: Start date for filtering releases
            until: End date for filtering releases
            max_results: Maximum number of releases to return
            
        Returns:
            List of Deployment objects
        """
        logger.info(f"Fetching releases for {self.owner}/{self.repo}")
        
        # Build GraphQL query
        query = gql("""
            query($owner: String!, $repo: String!, $cursor: String) {
                repository(owner: $owner, name: $repo) {
                    releases(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            tagName
                            name
                            createdAt
                            publishedAt
                            isPrerelease
                            tagCommit {
                                oid
                            }
                        }
                    }
                }
                rateLimit {
                    remaining
                    resetAt
                }
            }
        """)
        
        # Fetch all pages
        all_releases = []
        cursor = None
        
        while True:
            variables = {
                "owner": self.owner,
                "repo": self.repo,
                "cursor": cursor,
            }
            
            # Execute query with rate limit handling
            result = self._execute_with_rate_limit(query, variables)
            
            # Process results
            release_nodes = result["repository"]["releases"]["nodes"]
            for release_data in release_nodes:
                release = self._parse_release(release_data)
                
                # Apply date filtering
                if since and release.created_at < since:
                    continue
                if until and release.created_at > until:
                    continue
                    
                all_releases.append(release)
                
                # Check if we've hit the max results limit
                if max_results and len(all_releases) >= max_results:
                    logger.info(f"Reached max_results limit of {max_results}")
                    return all_releases[:max_results]
            
            # Check for more pages
            page_info = result["repository"]["releases"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
                
            cursor = page_info["endCursor"]
            
        logger.info(f"Fetched {len(all_releases)} releases")
        return all_releases
    
    def _execute_with_rate_limit(self, query, variables: Dict) -> Dict:
        """Execute GraphQL query with rate limit handling."""
        max_retries = 5
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                result = self.client.execute(query, variable_values=variables)
                
                # Check rate limit
                rate_limit = result.get("rateLimit", {})
                remaining = rate_limit.get("remaining", 0)
                
                if remaining < 10:
                    reset_at = rate_limit.get("resetAt")
                    if reset_at:
                        reset_time = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                        wait_time = (reset_time - datetime.now().astimezone()).total_seconds()
                        if wait_time > 0:
                            logger.warning(f"Rate limit low ({remaining} remaining), waiting {wait_time:.0f}s")
                            time.sleep(wait_time + 1)
                            
                return result
                
            except Exception as e:
                if "rate limit" in str(e).lower() or attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    raise
                    
        raise Exception("Max retries exceeded")
    
    def _parse_pull_request(self, pr_data: Dict) -> PullRequest:
        """Parse PR data from GraphQL response."""
        # Determine PR state
        if pr_data.get("mergedAt"):
            state = PRState.MERGED
        elif pr_data.get("closedAt"):
            state = PRState.CLOSED
        else:
            state = PRState.OPEN
            
        # Extract commit SHAs
        commits = []
        for commit_edge in pr_data.get("commits", {}).get("nodes", []):
            commits.append(commit_edge["commit"]["oid"])
            
        # Extract labels
        labels = []
        for label_edge in pr_data.get("labels", {}).get("nodes", []):
            labels.append(label_edge["name"])
            
        # Handle author - can be None if user deleted their account
        author = None
        if pr_data.get("author"):
            author = pr_data["author"].get("login")
            
        return PullRequest(
            number=pr_data["number"],
            title=pr_data["title"],
            state=state,
            created_at=datetime.fromisoformat(pr_data["createdAt"].replace("Z", "+00:00")),
            updated_at=datetime.fromisoformat(pr_data["updatedAt"].replace("Z", "+00:00")),
            closed_at=datetime.fromisoformat(pr_data["closedAt"].replace("Z", "+00:00")) if pr_data.get("closedAt") else None,
            merged_at=datetime.fromisoformat(pr_data["mergedAt"].replace("Z", "+00:00")) if pr_data.get("mergedAt") else None,
            merge_commit_sha=pr_data.get("mergeCommit", {}).get("oid") if pr_data.get("mergeCommit") else None,
            commits=commits,
            author=author,
            labels=labels,
        )
    
    def _parse_release(self, release_data: Dict) -> Deployment:
        """Parse release data from GraphQL response."""
        return Deployment(
            tag_name=release_data["tagName"],
            name=release_data.get("name") or release_data["tagName"],
            created_at=datetime.fromisoformat(release_data["createdAt"].replace("Z", "+00:00")),
            published_at=datetime.fromisoformat(release_data["publishedAt"].replace("Z", "+00:00")) if release_data.get("publishedAt") else None,
            commit_sha=release_data.get("tagCommit", {}).get("oid", ""),
            is_prerelease=release_data.get("isPrerelease", False),
        )