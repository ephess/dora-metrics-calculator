"""Git repository commit extractor."""

from datetime import datetime
from typing import List, Optional

from git import Repo
from git.exc import InvalidGitRepositoryError

from ..logging import get_logger
from ..models import Commit

logger = get_logger(__name__)


class GitExtractor:
    """Extract commit information from a git repository."""

    def __init__(self, repo_path: str):
        """
        Initialize GitExtractor with repository path.

        Args:
            repo_path: Path to the git repository

        Raises:
            ValueError: If the path is not a valid git repository
        """
        self.repo_path = repo_path
        try:
            self.repo = Repo(repo_path)
        except (InvalidGitRepositoryError, Exception) as e:
            logger.error(f"Failed to open git repository at {repo_path}: {e}")
            raise ValueError(f"Invalid git repository: {repo_path}")

    def extract_commits(
        self,
        branch: str = "main",
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        max_count: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> List[Commit]:
        """
        Extract commits from the repository.

        Args:
            branch: Branch name to extract commits from
            since: Start date for filtering commits
            until: End date for filtering commits
            max_count: Maximum number of commits to extract
            progress_callback: Optional callback function for progress updates

        Returns:
            List of Commit objects
        """
        logger.info(f"Extracting commits from branch '{branch}'")
        
        # Build kwargs for iter_commits
        kwargs = {"max_count": max_count}
        if since:
            kwargs["since"] = since.strftime("%Y-%m-%d")
        if until:
            kwargs["until"] = until.strftime("%Y-%m-%d")
        
        commits = []
        try:
            # Get total count if possible for progress
            total_commits = None
            if progress_callback:
                try:
                    total_commits = sum(1 for _ in self.repo.iter_commits(branch, **kwargs))
                    kwargs_for_iter = kwargs.copy()  # Don't modify original kwargs
                except:
                    # If counting fails, continue without progress
                    kwargs_for_iter = kwargs
            else:
                kwargs_for_iter = kwargs
                
            for i, git_commit in enumerate(self.repo.iter_commits(branch, **kwargs_for_iter)):
                commit = self._convert_git_commit(git_commit)
                commits.append(commit)
                
                # Call progress callback if provided
                if progress_callback and total_commits:
                    progress = (i + 1) / total_commits
                    progress_callback(progress)
                
            logger.info(f"Extracted {len(commits)} commits from branch '{branch}'")
            return commits
            
        except Exception as e:
            # Handle case where branch doesn't exist or has no commits
            if "bad revision" in str(e) or "unknown revision" in str(e):
                logger.warning(f"Branch '{branch}' not found or has no commits")
                return []
            logger.error(f"Failed to extract commits: {e}")
            raise

    def _convert_git_commit(self, git_commit) -> Commit:
        """Convert GitPython commit object to our Commit model."""
        # Get file statistics
        files_changed = list(git_commit.stats.files.keys())
        additions = git_commit.stats.total.get("insertions", 0)
        deletions = git_commit.stats.total.get("deletions", 0)
        
        return Commit(
            sha=git_commit.hexsha,
            author_name=git_commit.author.name,
            author_email=git_commit.author.email,
            authored_date=git_commit.authored_datetime,
            committer_name=git_commit.committer.name,
            committer_email=git_commit.committer.email,
            committed_date=git_commit.committed_datetime,
            message=git_commit.message.strip(),
            files_changed=files_changed,
            additions=additions,
            deletions=deletions,
        )

    def get_branches(self) -> List[str]:
        """
        Get list of all branches in the repository.

        Returns:
            List of branch names
        """
        return [branch.name for branch in self.repo.branches]

    def get_default_branch(self) -> str:
        """
        Get the default branch of the repository.

        Returns:
            Name of the default branch
        """
        try:
            return self.repo.active_branch.name
        except TypeError:
            # Detached HEAD state
            return "main"  # Fallback to main