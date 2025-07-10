"""Unit tests for git extractor."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dora_metrics.extractors.git_extractor import GitExtractor
from dora_metrics.models import Commit


@pytest.mark.unit
class TestGitExtractor:
    """Test GitExtractor with mocked git operations."""

    @pytest.fixture
    def mock_repo(self):
        """Create a mock git repository."""
        with patch("dora_metrics.extractors.git_extractor.Repo") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo_class.return_value = mock_repo
            yield mock_repo

    @pytest.fixture
    def extractor(self, mock_repo):
        """Create a GitExtractor instance with mocked repo."""
        extractor = GitExtractor("/fake/repo/path")
        extractor.repo = mock_repo
        return extractor

    def test_init_with_valid_repo(self):
        """Test initialization with a valid repository path."""
        with patch("dora_metrics.extractors.git_extractor.Repo") as mock_repo_class:
            mock_repo_class.return_value = MagicMock()
            extractor = GitExtractor("/fake/repo/path")
            assert extractor.repo_path == "/fake/repo/path"
            mock_repo_class.assert_called_once_with("/fake/repo/path")

    def test_init_with_invalid_repo(self):
        """Test initialization with an invalid repository path."""
        with patch("dora_metrics.extractors.git_extractor.Repo") as mock_repo_class:
            mock_repo_class.side_effect = Exception("Not a git repository")
            with pytest.raises(ValueError, match="Invalid git repository"):
                GitExtractor("/invalid/path")

    def test_extract_commits_from_branch(self, extractor, mock_repo):
        """Test extracting commits from a specific branch."""
        # Create mock commits
        mock_commit1 = self._create_mock_commit(
            "abc123",
            "Author 1",
            "author1@example.com",
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            "Fix bug",
        )
        mock_commit2 = self._create_mock_commit(
            "def456",
            "Author 2",
            "author2@example.com",
            datetime(2024, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
            "Add feature",
        )
        
        mock_repo.iter_commits.return_value = [mock_commit1, mock_commit2]
        
        commits = extractor.extract_commits(branch="main")
        
        assert len(commits) == 2
        assert commits[0].sha == "abc123"
        assert commits[0].author_name == "Author 1"
        assert commits[1].sha == "def456"
        mock_repo.iter_commits.assert_called_once_with("main", max_count=None)

    def test_extract_commits_with_date_filter(self, extractor, mock_repo):
        """Test extracting commits with date filtering."""
        mock_commit = self._create_mock_commit(
            "abc123",
            "Author 1",
            "author1@example.com",
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            "Fix bug",
        )
        
        mock_repo.iter_commits.return_value = [mock_commit]
        
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 2, 1, tzinfo=timezone.utc)
        
        commits = extractor.extract_commits(branch="main", since=since, until=until)
        
        assert len(commits) == 1
        assert commits[0].sha == "abc123"
        mock_repo.iter_commits.assert_called_once_with(
            "main", 
            since=since.strftime("%Y-%m-%d"),
            until=until.strftime("%Y-%m-%d"),
            max_count=None
        )

    def test_extract_commits_empty_repo(self, extractor, mock_repo):
        """Test extracting commits from an empty repository."""
        mock_repo.iter_commits.return_value = []
        
        commits = extractor.extract_commits(branch="main")
        
        assert len(commits) == 0
        assert commits == []

    def test_extract_commits_with_file_stats(self, extractor, mock_repo):
        """Test extracting commits with file statistics."""
        mock_commit = self._create_mock_commit(
            "abc123",
            "Author 1",
            "author1@example.com",
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            "Fix bug",
            files=["file1.py", "file2.py"],
            additions=10,
            deletions=5,
        )
        
        mock_repo.iter_commits.return_value = [mock_commit]
        
        commits = extractor.extract_commits(branch="main")
        
        assert len(commits) == 1
        assert commits[0].files_changed == ["file1.py", "file2.py"]
        assert commits[0].additions == 10
        assert commits[0].deletions == 5

    def test_get_branches(self, extractor, mock_repo):
        """Test getting list of branches."""
        mock_branch1 = MagicMock()
        mock_branch1.name = "main"
        mock_branch2 = MagicMock()
        mock_branch2.name = "develop"
        
        mock_repo.branches = [mock_branch1, mock_branch2]
        
        branches = extractor.get_branches()
        
        assert branches == ["main", "develop"]

    def test_get_default_branch(self, extractor, mock_repo):
        """Test getting default branch."""
        mock_repo.active_branch.name = "main"
        
        default_branch = extractor.get_default_branch()
        
        assert default_branch == "main"

    def _create_mock_commit(
        self,
        sha: str,
        author_name: str,
        author_email: str,
        authored_date: datetime,
        message: str,
        files: list = None,
        additions: int = 0,
        deletions: int = 0,
    ):
        """Helper to create a mock commit object."""
        mock_commit = MagicMock()
        mock_commit.hexsha = sha
        mock_commit.author.name = author_name
        mock_commit.author.email = author_email
        mock_commit.authored_datetime = authored_date
        mock_commit.committer.name = author_name  # Same as author for simplicity
        mock_commit.committer.email = author_email
        mock_commit.committed_datetime = authored_date
        mock_commit.message = message
        
        # Mock stats
        mock_commit.stats.total = {"insertions": additions, "deletions": deletions}
        if files:
            mock_commit.stats.files = {f: {"insertions": 0, "deletions": 0} for f in files}
        else:
            mock_commit.stats.files = {}
        
        return mock_commit