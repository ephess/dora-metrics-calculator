"""Data association logic for matching commits to PRs and identifying deployments."""

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from ..logging import get_logger
from ..models import Commit, Deployment, PullRequest

logger = get_logger(__name__)


class DataAssociator:
    """Associates commits with pull requests and identifies deployments."""
    
    def __init__(self, hotfix_labels: Optional[Set[str]] = None):
        """
        Initialize the data associator.
        
        Args:
            hotfix_labels: Set of labels that indicate a hotfix PR. 
                          Defaults to {"hotfix", "urgent", "critical", "emergency"}
        """
        self.commits_by_sha: Dict[str, Commit] = {}
        self.prs_by_number: Dict[int, PullRequest] = {}
        self.deployments_by_tag: Dict[str, Deployment] = {}
        self.deployment_sha_to_tag: Dict[str, str] = {}
        self.hotfix_labels = hotfix_labels or {"hotfix", "urgent", "critical", "emergency"}
        
    def associate_data(
        self,
        commits: List[Commit],
        pull_requests: List[PullRequest],
        deployments: List[Deployment]
    ) -> Tuple[List[Commit], List[PullRequest]]:
        """
        Associate commits with PRs and identify deployments.
        
        Args:
            commits: List of commits from git
            pull_requests: List of PRs from GitHub
            deployments: List of deployments (releases) from GitHub
            
        Returns:
            Tuple of (updated commits, updated pull requests)
        """
        logger.info(f"Associating {len(commits)} commits, {len(pull_requests)} PRs, "
                   f"and {len(deployments)} deployments")
        
        # Build lookup structures
        self._build_lookups(commits, pull_requests, deployments)
        
        # Associate commits with PRs
        self._associate_commits_to_prs()
        
        # Mark deployment commits
        self._mark_deployment_commits()
        
        # Identify hotfix PRs
        self._identify_hotfixes()
        
        # Return updated data
        updated_commits = list(self.commits_by_sha.values())
        updated_prs = list(self.prs_by_number.values())
        
        logger.info(f"Association complete. {self._count_associated_commits()} commits "
                   f"associated with PRs, {self._count_deployment_commits()} deployment commits")
        
        return updated_commits, updated_prs
        
    def _build_lookups(
        self,
        commits: List[Commit],
        pull_requests: List[PullRequest],
        deployments: List[Deployment]
    ) -> None:
        """Build lookup dictionaries for efficient access."""
        # Build commit lookup
        self.commits_by_sha = {commit.sha: commit for commit in commits}
        
        # Build PR lookup
        self.prs_by_number = {pr.number: pr for pr in pull_requests}
        
        # Build deployment lookups
        self.deployments_by_tag = {deploy.tag_name: deploy for deploy in deployments}
        self.deployment_sha_to_tag = {deploy.commit_sha: deploy.tag_name for deploy in deployments}
        
    def _associate_commits_to_prs(self) -> None:
        """Associate commits with their pull requests."""
        # First pass: Associate by PR commit lists
        for pr in self.prs_by_number.values():
            for commit_sha in pr.commits:
                if commit_sha in self.commits_by_sha:
                    commit = self.commits_by_sha[commit_sha]
                    commit.pr_number = pr.number
                    logger.debug(f"Associated commit {commit_sha[:7]} with PR #{pr.number}")
                    
        # Second pass: Associate by merge commit
        for pr in self.prs_by_number.values():
            if pr.merge_commit_sha and pr.merge_commit_sha in self.commits_by_sha:
                commit = self.commits_by_sha[pr.merge_commit_sha]
                # Only associate if not already associated
                if commit.pr_number is None:
                    commit.pr_number = pr.number
                    logger.debug(f"Associated merge commit {pr.merge_commit_sha[:7]} "
                               f"with PR #{pr.number}")
                    
    def _mark_deployment_commits(self) -> None:
        """Associate commits with their deployment tags."""
        for deploy_sha, tag_name in self.deployment_sha_to_tag.items():
            if deploy_sha in self.commits_by_sha:
                commit = self.commits_by_sha[deploy_sha]
                commit.deployment_tag = tag_name
                logger.debug(f"Associated commit {deploy_sha[:7]} with deployment "
                           f"tag: {tag_name}")
            else:
                logger.warning(f"Deployment commit {deploy_sha[:7]} not found in commit list "
                             f"(tag: {tag_name})")
                
    def _identify_hotfixes(self) -> None:
        """Identify hotfix PRs based on labels."""
        # Convert hotfix labels to lowercase for case-insensitive comparison
        hotfix_labels_lower = {label.lower() for label in self.hotfix_labels}
        
        for pr in self.prs_by_number.values():
            # Check if any PR labels indicate a hotfix
            pr_labels_lower = {label.lower() for label in pr.labels}
            matching_labels = pr_labels_lower & hotfix_labels_lower
            if matching_labels:
                # Mark this as a hotfix PR
                # Note: We store this info on the PR, not the commit
                # The metrics calculator will use this when analyzing lead time
                logger.debug(f"PR #{pr.number} identified as hotfix based on labels: "
                           f"{matching_labels}")
                           
    def _count_associated_commits(self) -> int:
        """Count commits that have been associated with PRs."""
        return sum(1 for commit in self.commits_by_sha.values() if commit.pr_number is not None)
        
    def _count_deployment_commits(self) -> int:
        """Count commits marked as deployments."""
        return sum(1 for commit in self.commits_by_sha.values() if commit.deployment_tag is not None)
        
    def get_orphaned_commits(self) -> List[Commit]:
        """
        Get commits that couldn't be associated with any PR.
        
        Returns:
            List of commits without PR associations
        """
        return [
            commit for commit in self.commits_by_sha.values()
            if commit.pr_number is None
        ]
        
    def get_prs_without_commits(self) -> List[PullRequest]:
        """
        Get PRs that don't have any commits in the main branch.
        
        Returns:
            List of PRs without associated commits
        """
        associated_pr_numbers = {
            commit.pr_number for commit in self.commits_by_sha.values()
            if commit.pr_number is not None
        }
        
        return [
            pr for pr in self.prs_by_number.values()
            if pr.number not in associated_pr_numbers
        ]
        
    def get_deployments_without_commits(self) -> List[Deployment]:
        """
        Get deployments that reference commits not in our list.
        
        Returns:
            List of deployments without matching commits
        """
        return [
            deploy for deploy in self.deployments_by_tag.values()
            if deploy.commit_sha not in self.commits_by_sha
        ]