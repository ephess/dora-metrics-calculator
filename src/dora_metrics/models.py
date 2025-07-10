"""Data models for DORA metrics tool."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


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