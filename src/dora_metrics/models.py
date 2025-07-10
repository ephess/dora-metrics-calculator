"""Data models for DORA metrics tool."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class PRState(Enum):
    """Pull request state."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    MERGED = "MERGED"


@dataclass
class PullRequest:
    """Represents a GitHub pull request."""
    
    number: int
    title: str
    state: PRState
    created_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime]
    merged_at: Optional[datetime]
    merge_commit_sha: Optional[str]
    commits: List[str] = field(default_factory=list)  # List of commit SHAs
    author: Optional[str] = None
    labels: List[str] = field(default_factory=list)  # PR labels
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "number": self.number,
            "title": self.title,
            "state": self.state.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "merged_at": self.merged_at.isoformat() if self.merged_at else None,
            "merge_commit_sha": self.merge_commit_sha,
            "commits": self.commits,
            "author": self.author,
            "labels": self.labels,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "PullRequest":
        """Create from dictionary."""
        return cls(
            number=data["number"],
            title=data["title"],
            state=PRState(data["state"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            closed_at=datetime.fromisoformat(data["closed_at"]) if data.get("closed_at") else None,
            merged_at=datetime.fromisoformat(data["merged_at"]) if data.get("merged_at") else None,
            merge_commit_sha=data.get("merge_commit_sha"),
            commits=data.get("commits", []),
            author=data.get("author"),
            labels=data.get("labels", []),
        )


@dataclass
class Deployment:
    """Represents a deployment (GitHub release)."""
    
    tag_name: str
    name: str
    created_at: datetime
    published_at: Optional[datetime]
    commit_sha: str
    is_prerelease: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "tag_name": self.tag_name,
            "name": self.name,
            "created_at": self.created_at.isoformat(),
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "commit_sha": self.commit_sha,
            "is_prerelease": self.is_prerelease,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Deployment":
        """Create from dictionary."""
        return cls(
            tag_name=data["tag_name"],
            name=data["name"],
            created_at=datetime.fromisoformat(data["created_at"]),
            published_at=datetime.fromisoformat(data["published_at"]) if data.get("published_at") else None,
            commit_sha=data["commit_sha"],
            is_prerelease=data.get("is_prerelease", False),
        )


@dataclass
class Commit:
    """Represents a git commit."""

    sha: str
    author_name: str
    author_email: str
    authored_date: datetime
    committer_name: str
    committer_email: str
    committed_date: datetime
    message: str
    files_changed: List[str] = field(default_factory=list)
    additions: int = 0
    deletions: int = 0
    
    # PR association (to be filled later)
    pr_number: Optional[int] = None
    
    # Deployment info (to be filled later)
    is_deployment: bool = False
    deployment_date: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "sha": self.sha,
            "author_name": self.author_name,
            "author_email": self.author_email,
            "authored_date": self.authored_date.isoformat(),
            "committer_name": self.committer_name,
            "committer_email": self.committer_email,
            "committed_date": self.committed_date.isoformat(),
            "message": self.message,
            "files_changed": self.files_changed,
            "additions": self.additions,
            "deletions": self.deletions,
            "pr_number": self.pr_number,
            "is_deployment": self.is_deployment,
            "deployment_date": self.deployment_date.isoformat() if self.deployment_date else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Commit":
        """Create from dictionary."""
        return cls(
            sha=data["sha"],
            author_name=data["author_name"],
            author_email=data["author_email"],
            authored_date=datetime.fromisoformat(data["authored_date"]),
            committer_name=data["committer_name"],
            committer_email=data["committer_email"],
            committed_date=datetime.fromisoformat(data["committed_date"]),
            message=data["message"],
            files_changed=data.get("files_changed", []),
            additions=data.get("additions", 0),
            deletions=data.get("deletions", 0),
            pr_number=data.get("pr_number"),
            is_deployment=data.get("is_deployment", False),
            deployment_date=datetime.fromisoformat(data["deployment_date"]) 
                          if data.get("deployment_date") else None,
        )