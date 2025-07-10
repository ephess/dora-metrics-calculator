"""Script to create a test git repository for testing."""

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from git import Actor, Repo


def create_test_repository(path: str = None) -> tuple[Repo, str]:
    """
    Create a test git repository with sample commits.
    
    Returns:
        Tuple of (repo object, repo path)
    """
    if path is None:
        temp_dir = tempfile.mkdtemp()
        path = temp_dir
    
    repo = Repo.init(path)
    
    # Configure git user
    author = Actor("Test User", "test@example.com")
    committer = Actor("Test Committer", "committer@example.com")
    
    # Create some commits with different dates
    base_date = datetime.now(timezone.utc) - timedelta(days=30)
    
    # Initial commit
    file1 = Path(path) / "README.md"
    file1.write_text("# Test Project\n\nThis is a test repository.")
    repo.index.add(["README.md"])
    repo.index.commit(
        "Initial commit",
        author=author,
        committer=committer,
        author_date=base_date,
        commit_date=base_date,
    )
    
    # Add source file
    src_dir = Path(path) / "src"
    src_dir.mkdir()
    src_file = src_dir / "main.py"
    src_file.write_text('def main():\n    print("Hello, World!")\n')
    repo.index.add(["src/main.py"])
    repo.index.commit(
        "Add main.py",
        author=author,
        committer=committer,
        author_date=base_date + timedelta(days=2),
        commit_date=base_date + timedelta(days=2),
    )
    
    # Add tests
    test_dir = Path(path) / "tests"
    test_dir.mkdir()
    test_file = test_dir / "test_main.py"
    test_file.write_text("def test_main():\n    assert True\n")
    repo.index.add(["tests/test_main.py"])
    repo.index.commit(
        "Add tests",
        author=author,
        committer=committer,
        author_date=base_date + timedelta(days=5),
        commit_date=base_date + timedelta(days=5),
    )
    
    # Bug fix commit
    src_file.write_text('def main():\n    print("Hello, World!")\n    return 0\n')
    repo.index.add(["src/main.py"])
    repo.index.commit(
        "Fix: Add return value to main function",
        author=author,
        committer=committer,
        author_date=base_date + timedelta(days=10),
        commit_date=base_date + timedelta(days=10),
    )
    
    # Feature branch
    feature_branch = repo.create_head("feature/new-feature")
    feature_branch.checkout()
    
    feature_file = Path(path) / "src" / "feature.py"
    feature_file.write_text('def new_feature():\n    return "New feature!"\n')
    repo.index.add(["src/feature.py"])
    repo.index.commit(
        "Add new feature",
        author=author,
        committer=committer,
        author_date=base_date + timedelta(days=15),
        commit_date=base_date + timedelta(days=15),
    )
    
    # Switch back to main
    repo.heads.main.checkout()
    
    return repo, path


if __name__ == "__main__":
    # Create a test repo for manual testing
    repo, path = create_test_repository()
    print(f"Created test repository at: {path}")
    print(f"Branches: {[b.name for b in repo.branches]}")
    print(f"Commits on main: {len(list(repo.iter_commits('main')))}")
    print(f"Commits on feature: {len(list(repo.iter_commits('feature/new-feature')))}")