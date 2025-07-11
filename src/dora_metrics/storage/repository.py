"""Repository layer for data access using storage manager."""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from ..models import Commit, Deployment, PullRequest
from .storage_manager import StorageManager


class DataRepository:
    """Data repository for accessing DORA metrics data."""
    
    def __init__(self, storage_manager: StorageManager):
        """
        Initialize repository with a storage manager.
        
        Args:
            storage_manager: Storage manager instance to use
        """
        self.storage = storage_manager
    
    def save_commits(self, repo_name: str, commits: List[Commit]) -> None:
        """Save commits for a repository."""
        data = [commit.to_dict() for commit in commits]
        path = f"{repo_name}/commits.json"
        self.storage.write_json(path, data)
    
    def load_commits(self, repo_name: str) -> List[Commit]:
        """Load commits for a repository."""
        path = f"{repo_name}/commits.json"
        if not self.storage.exists(path):
            return []
        data = self.storage.read_json(path)
        return [Commit.from_dict(item) for item in data]
    
    def save_pull_requests(self, repo_name: str, prs: List[PullRequest]) -> None:
        """Save pull requests for a repository."""
        data = [pr.to_dict() for pr in prs]
        path = f"{repo_name}/pull_requests.json"
        self.storage.write_json(path, data)
    
    def load_pull_requests(self, repo_name: str) -> List[PullRequest]:
        """Load pull requests for a repository."""
        path = f"{repo_name}/pull_requests.json"
        if not self.storage.exists(path):
            return []
        data = self.storage.read_json(path)
        return [PullRequest.from_dict(item) for item in data]
    
    def save_deployments(self, repo_name: str, deployments: List[Deployment]) -> None:
        """Save deployments for a repository."""
        data = [deployment.to_dict() for deployment in deployments]
        path = f"{repo_name}/deployments.json"
        self.storage.write_json(path, data)
    
    def load_deployments(self, repo_name: str) -> List[Deployment]:
        """Load deployments for a repository."""
        path = f"{repo_name}/deployments.json"
        if not self.storage.exists(path):
            return []
        data = self.storage.read_json(path)
        return [Deployment.from_dict(item) for item in data]
    
    def save_metadata(self, repo_name: str, metadata: Dict) -> None:
        """Save metadata for a repository."""
        path = f"{repo_name}/metadata.json"
        self.storage.write_json(path, metadata)
    
    def load_metadata(self, repo_name: str) -> Dict:
        """Load metadata for a repository."""
        path = f"{repo_name}/metadata.json"
        if not self.storage.exists(path):
            return {}
        return self.storage.read_json(path)
    
    def update_metadata(self, repo_name: str, updates: Dict) -> None:
        """Update metadata for a repository."""
        metadata = self.load_metadata(repo_name)
        metadata.update(updates)
        metadata['last_update'] = datetime.now().isoformat()
        self.save_metadata(repo_name, metadata)
    
    def list_repositories(self) -> List[str]:
        """List all repositories with data."""
        repos = set()
        for file_path in self.storage.list():
            # Extract repo name from paths like "repo-name/commits.json"
            parts = file_path.split('/')
            if len(parts) >= 2:
                repos.add(parts[0])
        return sorted(repos)
    
    def repository_exists(self, repo_name: str) -> bool:
        """Check if a repository has any data."""
        return (
            self.storage.exists(f"{repo_name}/commits.json") or
            self.storage.exists(f"{repo_name}/pull_requests.json") or
            self.storage.exists(f"{repo_name}/deployments.json")
        )