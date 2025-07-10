"""Integration tests for git extractor with real repositories."""

import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from git import Repo

from dora_metrics.extractors.git_extractor import GitExtractor

# Add fixtures directory to path so we can import create_test_repo
sys.path.insert(0, str(Path(__file__).parent.parent / "fixtures"))
from create_test_repo import create_test_repository


@pytest.mark.integration
class TestGitExtractorIntegration:
    """Integration tests using real git repositories."""

    @pytest.fixture
    def temp_repo(self):
        """Create a temporary git repository for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Repo.init(temp_dir)
            
            # Configure git user for commits
            repo.config_writer().set_value("user", "name", "Test User").release()
            repo.config_writer().set_value("user", "email", "test@example.com").release()
            
            yield repo, temp_dir

    def test_extract_commits_from_real_repo(self, temp_repo):
        """Test extracting commits from a real repository."""
        repo, repo_path = temp_repo
        
        # Create some commits
        file1 = Path(repo_path) / "file1.txt"
        file1.write_text("Hello World")
        repo.index.add(["file1.txt"])
        commit1 = repo.index.commit("Initial commit")
        
        file2 = Path(repo_path) / "file2.txt"
        file2.write_text("Second file")
        repo.index.add(["file2.txt"])
        commit2 = repo.index.commit("Add second file")
        
        # Extract commits
        extractor = GitExtractor(repo_path)
        # Get the default branch (main or master)
        default_branch = repo.active_branch.name
        commits = extractor.extract_commits(branch=default_branch)
        
        assert len(commits) == 2
        # Commits are returned in reverse chronological order
        assert commits[0].message == "Add second file"
        assert commits[1].message == "Initial commit"
        assert commits[0].author_name == "Test User"
        assert commits[0].author_email == "test@example.com"

    def test_extract_commits_with_date_filter_real_repo(self, temp_repo):
        """Test date filtering with a real repository."""
        repo, repo_path = temp_repo
        
        # Create a commit
        file1 = Path(repo_path) / "file1.txt"
        file1.write_text("Hello World")
        repo.index.add(["file1.txt"])
        commit = repo.index.commit("Test commit")
        
        # Extract with date filter
        extractor = GitExtractor(repo_path)
        
        # Should find the commit
        now = datetime.now(timezone.utc)
        yesterday = datetime(now.year, now.month, now.day - 1, tzinfo=timezone.utc)
        tomorrow = datetime(now.year, now.month, now.day + 1, tzinfo=timezone.utc)
        
        default_branch = repo.active_branch.name
        commits = extractor.extract_commits(branch=default_branch, since=yesterday, until=tomorrow)
        assert len(commits) == 1
        
        # Should not find the commit
        last_week = datetime(now.year, now.month, now.day - 7, tzinfo=timezone.utc)
        two_days_ago = datetime(now.year, now.month, now.day - 2, tzinfo=timezone.utc)
        
        commits = extractor.extract_commits(branch=default_branch, since=last_week, until=two_days_ago)
        assert len(commits) == 0

    def test_extract_commits_with_file_changes(self, temp_repo):
        """Test extracting commits with file change statistics."""
        repo, repo_path = temp_repo
        
        # Create a commit with multiple file changes
        file1 = Path(repo_path) / "src" / "module.py"
        file1.parent.mkdir(parents=True)
        file1.write_text("def hello():\n    return 'Hello'\n")
        
        file2 = Path(repo_path) / "README.md"
        file2.write_text("# Test Project\n\nThis is a test.\n")
        
        repo.index.add(["src/module.py", "README.md"])
        commit = repo.index.commit("Add module and README")
        
        # Extract and verify
        extractor = GitExtractor(repo_path)
        default_branch = repo.active_branch.name
        commits = extractor.extract_commits(branch=default_branch)
        
        assert len(commits) == 1
        assert set(commits[0].files_changed) == {"src/module.py", "README.md"}
        assert commits[0].additions > 0  # Should have additions

    def test_empty_repository(self, temp_repo):
        """Test extracting from an empty repository."""
        repo, repo_path = temp_repo
        
        extractor = GitExtractor(repo_path)
        # Try to get commits from default branch - should handle gracefully
        commits = extractor.extract_commits(branch="main")
        
        assert len(commits) == 0

    def test_multiple_branches(self, temp_repo):
        """Test extracting from different branches."""
        repo, repo_path = temp_repo
        
        # Get default branch name
        default_branch = repo.active_branch.name
        
        # Create commit on default branch
        file1 = Path(repo_path) / "main_file.txt"
        file1.write_text("Main branch")
        repo.index.add(["main_file.txt"])
        repo.index.commit("Main commit")
        
        # Create and switch to feature branch
        feature_branch = repo.create_head("feature")
        feature_branch.checkout()
        
        file2 = Path(repo_path) / "feature_file.txt"
        file2.write_text("Feature branch")
        repo.index.add(["feature_file.txt"])
        repo.index.commit("Feature commit")
        
        extractor = GitExtractor(repo_path)
        
        # Check branches
        branches = extractor.get_branches()
        assert default_branch in branches
        assert "feature" in branches
        
        # Extract from default branch
        main_commits = extractor.extract_commits(branch=default_branch)
        assert len(main_commits) == 1
        assert main_commits[0].message == "Main commit"
        
        # Extract from feature (should have both commits)
        feature_commits = extractor.extract_commits(branch="feature")
        assert len(feature_commits) == 2

    def test_extract_from_current_repo(self):
        """Test extracting from the current project repository."""
        # Get the root of our project
        current_file = Path(__file__)
        repo_root = current_file.parent.parent.parent  # Go up to project root
        
        extractor = GitExtractor(str(repo_root))
        
        # Just check we can extract some commits
        commits = extractor.extract_commits(branch="main", max_count=5)
        
        assert len(commits) > 0
        assert all(commit.sha for commit in commits)
        assert all(commit.message for commit in commits)
        
        # Verify default branch detection
        default_branch = extractor.get_default_branch()
        assert default_branch in ["main", "master"]

    def test_with_comprehensive_test_repository(self):
        """Test using our comprehensive test repository fixture."""
        repo, repo_path = create_test_repository()
        
        try:
            extractor = GitExtractor(repo_path)
            
            # Test extracting from main branch
            main_commits = extractor.extract_commits(branch="main")
            assert len(main_commits) == 4
            
            # Verify commit messages
            messages = [c.message for c in main_commits]
            assert "Initial commit" in messages
            assert "Add main.py" in messages
            assert "Add tests" in messages
            assert "Fix: Add return value to main function" in messages
            
            # Test extracting from feature branch
            feature_commits = extractor.extract_commits(branch="feature/new-feature")
            assert len(feature_commits) == 5  # 4 from main + 1 new
            assert "Add new feature" in [c.message for c in feature_commits]
            
            # Test date filtering - get commits from last 26 days
            filter_date = datetime.now(timezone.utc) - timedelta(days=26)
            filtered_commits = extractor.extract_commits(branch="main", since=filter_date)
            # Should get commits from 25, 20 days ago (2 commits: tests and fix)
            assert len(filtered_commits) == 2
            assert "Initial commit" not in [c.message for c in filtered_commits]
            assert "Add main.py" not in [c.message for c in filtered_commits]
            
            # Test that commits have proper file statistics
            for commit in main_commits:
                assert isinstance(commit.files_changed, list)
                assert commit.additions >= 0
                assert commit.deletions >= 0
            
            # Test branch listing
            branches = extractor.get_branches()
            assert "main" in branches
            assert "feature/new-feature" in branches
            
        finally:
            # Clean up temporary directory
            import shutil
            shutil.rmtree(repo_path)