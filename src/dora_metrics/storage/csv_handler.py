"""CSV handler for exporting and importing DORA metrics data."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from dateutil import parser as date_parser

from ..logging import get_logger
from ..models import Commit, Deployment, PRState, PullRequest

logger = get_logger(__name__)


class CSVHandler:
    """Handles CSV export and import operations for DORA metrics data."""
    
    # CSV column definitions
    COMMIT_COLUMNS = [
        "sha",
        "author_name",
        "author_email",
        "authored_date",
        "committer_name",
        "committer_email",
        "committed_date",
        "message",
        "files_changed",
        "additions",
        "deletions",
        "pr_number",
        "deployment_tag",  # From GitHub releases
        # Annotation columns
        "is_manual_deployment",  # For historical deployments not in GitHub
        "manual_deployment_timestamp",  # Optional deployment time (defaults to commit time)
        "manual_deployment_failed",  # Whether the manual deployment failed
        "notes",
    ]
    
    PR_COLUMNS = [
        "number",
        "title",
        "state",
        "created_at",
        "updated_at",
        "closed_at",
        "merged_at",
        "merge_commit_sha",
        "commits",
        "author",
        "labels",
        # Annotation columns
        "is_hotfix",  # Pre-populated based on labels, can be modified
        "notes",
    ]
    
    DEPLOYMENT_COLUMNS = [
        "tag_name",
        "name",
        "created_at",
        "published_at",
        "commit_sha",
        "is_prerelease",
        # Annotation columns
        "deployment_failed",
        "failure_resolved_at",
        "notes",
    ]
    
    def __init__(self, encoding: str = "utf-8-sig", hotfix_labels: Optional[Set[str]] = None):
        """
        Initialize CSV handler.
        
        Args:
            encoding: File encoding to use (default utf-8-sig for Excel compatibility)
            hotfix_labels: Set of labels that indicate a hotfix PR
        """
        self.encoding = encoding
        self.hotfix_labels = hotfix_labels or {"hotfix", "urgent", "critical", "emergency"}
        
    def export_commits(self, commits: List[Commit], filepath: Path) -> None:
        """
        Export commits to CSV file.
        
        Args:
            commits: List of commits to export
            filepath: Path to output CSV file
        """
        logger.info(f"Exporting {len(commits)} commits to {filepath}")
        
        rows = []
        for commit in commits:
            row = {
                "sha": commit.sha,
                "author_name": commit.author_name,
                "author_email": commit.author_email,
                "authored_date": commit.authored_date.isoformat(),
                "committer_name": commit.committer_name,
                "committer_email": commit.committer_email,
                "committed_date": commit.committed_date.isoformat(),
                "message": commit.message.replace("\n", " "),  # Single line for CSV
                "files_changed": "|".join(commit.files_changed),  # Pipe-delimited
                "additions": commit.additions,
                "deletions": commit.deletions,
                "pr_number": commit.pr_number or "",
                "deployment_tag": commit.deployment_tag or "",
                "is_manual_deployment": str(getattr(commit, "is_manual_deployment", "")).lower() 
                                       if hasattr(commit, "is_manual_deployment") and 
                                          getattr(commit, "is_manual_deployment") is not None else "",
                "manual_deployment_timestamp": getattr(commit, "manual_deployment_timestamp", "").isoformat() 
                                              if hasattr(commit, "manual_deployment_timestamp") and 
                                                 getattr(commit, "manual_deployment_timestamp") else "",
                "manual_deployment_failed": str(getattr(commit, "manual_deployment_failed", "")).lower() 
                                           if hasattr(commit, "manual_deployment_failed") and 
                                              getattr(commit, "manual_deployment_failed") is not None else "",
                "notes": getattr(commit, "notes", ""),
            }
            rows.append(row)
            
        self._write_csv(filepath, self.COMMIT_COLUMNS, rows)
        
    def export_pull_requests(self, pull_requests: List[PullRequest], filepath: Path) -> None:
        """
        Export pull requests to CSV file with auto-detected hotfix status.
        
        Args:
            pull_requests: List of PRs to export
            filepath: Path to output CSV file
        """
        logger.info(f"Exporting {len(pull_requests)} pull requests to {filepath}")
        
        rows = []
        for pr in pull_requests:
            # Auto-detect hotfix based on labels
            is_hotfix = self._detect_hotfix(pr.labels)
            
            row = {
                "number": pr.number,
                "title": pr.title,
                "state": pr.state.value,
                "created_at": pr.created_at.isoformat(),
                "updated_at": pr.updated_at.isoformat(),
                "closed_at": pr.closed_at.isoformat() if pr.closed_at else "",
                "merged_at": pr.merged_at.isoformat() if pr.merged_at else "",
                "merge_commit_sha": pr.merge_commit_sha or "",
                "commits": "|".join(pr.commits),  # Pipe-delimited
                "author": pr.author or "",
                "labels": "|".join(pr.labels),  # Pipe-delimited
                "is_hotfix": str(is_hotfix).lower() if is_hotfix is not None else "",
                "notes": getattr(pr, "notes", ""),
            }
            rows.append(row)
            
        self._write_csv(filepath, self.PR_COLUMNS, rows)
        
    def export_deployments(self, deployments: List[Deployment], filepath: Path) -> None:
        """
        Export deployments to CSV file.
        
        Args:
            deployments: List of deployments to export
            filepath: Path to output CSV file
        """
        logger.info(f"Exporting {len(deployments)} deployments to {filepath}")
        
        rows = []
        for deployment in deployments:
            row = {
                "tag_name": deployment.tag_name,
                "name": deployment.name,
                "created_at": deployment.created_at.isoformat(),
                "published_at": deployment.published_at.isoformat() if deployment.published_at else "",
                "commit_sha": deployment.commit_sha,
                "is_prerelease": str(deployment.is_prerelease).lower(),
                "deployment_failed": str(getattr(deployment, "deployment_failed", "")).lower() 
                                   if hasattr(deployment, "deployment_failed") else "",
                "failure_resolved_at": getattr(deployment, "failure_resolved_at", "").isoformat() 
                                     if hasattr(deployment, "failure_resolved_at") and 
                                        getattr(deployment, "failure_resolved_at") else "",
                "notes": getattr(deployment, "notes", ""),
            }
            rows.append(row)
            
        self._write_csv(filepath, self.DEPLOYMENT_COLUMNS, rows)
        
    def import_commits(self, filepath: Path) -> List[Commit]:
        """
        Import commits from CSV file.
        
        Args:
            filepath: Path to input CSV file
            
        Returns:
            List of commits with annotations
        """
        logger.info(f"Importing commits from {filepath}")
        
        rows = self._read_csv(filepath)
        commits = []
        
        for row in rows:
            # Parse commit data
            commit = Commit(
                sha=row["sha"],
                author_name=row["author_name"],
                author_email=row["author_email"],
                authored_date=self._parse_datetime(row["authored_date"]),
                committer_name=row["committer_name"],
                committer_email=row["committer_email"],
                committed_date=self._parse_datetime(row["committed_date"]),
                message=row["message"],
                files_changed=row["files_changed"].split("|") if row["files_changed"] else [],
                additions=int(row["additions"]) if row["additions"] else 0,
                deletions=int(row["deletions"]) if row["deletions"] else 0,
                pr_number=int(row["pr_number"]) if row["pr_number"] else None,
                deployment_tag=row["deployment_tag"] if row["deployment_tag"] else None,
            )
            
            # Add annotation attributes
            commit.is_manual_deployment = self._parse_bool(row.get("is_manual_deployment", ""))
            
            # Parse manual deployment timestamp if provided
            if row.get("manual_deployment_timestamp", "").strip():
                commit.manual_deployment_timestamp = self._parse_datetime(row["manual_deployment_timestamp"])
            else:
                # Default to commit timestamp if marked as manual deployment but no timestamp given
                if commit.is_manual_deployment:
                    commit.manual_deployment_timestamp = commit.committed_date
                else:
                    commit.manual_deployment_timestamp = None
                    
            commit.manual_deployment_failed = self._parse_bool(row.get("manual_deployment_failed", ""))
            commit.notes = row.get("notes", "")
            
            commits.append(commit)
                
        logger.info(f"Imported {len(commits)} commits")
        return commits
        
    def import_pull_requests(self, filepath: Path) -> List[PullRequest]:
        """
        Import pull requests from CSV file.
        
        Args:
            filepath: Path to input CSV file
            
        Returns:
            List of pull requests with annotations
        """
        logger.info(f"Importing pull requests from {filepath}")
        
        rows = self._read_csv(filepath)
        pull_requests = []
        
        for row in rows:
            # Parse PR data
            pr = PullRequest(
                number=int(row["number"]),
                title=row["title"],
                state=PRState(row["state"]),
                created_at=self._parse_datetime(row["created_at"]),
                updated_at=self._parse_datetime(row["updated_at"]),
                closed_at=self._parse_datetime(row["closed_at"]) if row["closed_at"] else None,
                merged_at=self._parse_datetime(row["merged_at"]) if row["merged_at"] else None,
                merge_commit_sha=row["merge_commit_sha"] if row["merge_commit_sha"] else None,
                commits=row["commits"].split("|") if row["commits"] else [],
                author=row["author"] if row["author"] else None,
                labels=row["labels"].split("|") if row["labels"] else [],
            )
            
            # Add annotation attributes
            pr.is_hotfix = self._parse_bool(row.get("is_hotfix", ""))
            pr.notes = row.get("notes", "")
            
            pull_requests.append(pr)
                
        logger.info(f"Imported {len(pull_requests)} PRs")
        return pull_requests
        
    def import_deployments(self, filepath: Path) -> List[Deployment]:
        """
        Import deployments from CSV file.
        
        Args:
            filepath: Path to input CSV file
            
        Returns:
            List of deployments with annotations
        """
        logger.info(f"Importing deployments from {filepath}")
        
        rows = self._read_csv(filepath)
        deployments = []
        
        for row in rows:
            # Parse deployment data
            deployment = Deployment(
                tag_name=row["tag_name"],
                name=row["name"],
                created_at=self._parse_datetime(row["created_at"]),
                published_at=self._parse_datetime(row["published_at"]) if row["published_at"] else None,
                commit_sha=row["commit_sha"],
                is_prerelease=self._parse_bool(row.get("is_prerelease", "false")),
            )
            
            # Add annotation attributes
            deployment.deployment_failed = self._parse_bool(row.get("deployment_failed", ""))
            
            if row.get("failure_resolved_at", "").strip():
                deployment.failure_resolved_at = self._parse_datetime(row["failure_resolved_at"])
            else:
                deployment.failure_resolved_at = None
                
            deployment.notes = row.get("notes", "")
            
            deployments.append(deployment)
                
        logger.info(f"Imported {len(deployments)} deployments")
        return deployments
        
    def _detect_hotfix(self, labels: List[str]) -> Optional[bool]:
        """
        Detect if PR is a hotfix based on labels.
        
        Args:
            labels: PR labels
            
        Returns:
            True if hotfix detected, False if not, None if no labels
        """
        if not labels:
            return None
            
        # Case-insensitive comparison
        labels_lower = {label.lower() for label in labels}
        hotfix_labels_lower = {label.lower() for label in self.hotfix_labels}
        
        return bool(labels_lower & hotfix_labels_lower)
        
    def _write_csv(self, filepath: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        """Write CSV file with proper encoding and formatting."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, "w", newline="", encoding=self.encoding) as f:
            writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(rows)
            
    def _read_csv(self, filepath: Path) -> List[Dict[str, str]]:
        """Read CSV file with proper encoding."""
        rows = []
        
        # Try pandas first (better at handling Excel-edited files)
        try:
            df = pd.read_csv(filepath, encoding=self.encoding, dtype=str, na_filter=False)
            return df.to_dict("records")
        except Exception as e:
            logger.warning(f"Failed to read with pandas, trying csv module: {e}")
            
        # Fallback to csv module
        with open(filepath, "r", encoding=self.encoding) as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Clean up Excel artifacts
                cleaned_row = {k: v.strip() for k, v in row.items() if k}
                rows.append(cleaned_row)
                
        return rows
        
    def _parse_datetime(self, value: str) -> datetime:
        """Parse datetime string flexibly."""
        if not value or not value.strip():
            raise ValueError("Empty datetime string")
            
        value = value.strip()
        try:
            # Try ISO format first
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            # Fall back to dateutil parser
            return date_parser.parse(value)
            
    def _parse_bool(self, value: str) -> Optional[bool]:
        """Parse boolean value from CSV."""
        if not value or not value.strip():
            return None
            
        value = value.strip().lower()
        if value in ("true", "yes", "y", "1", "x"):
            return True
        elif value in ("false", "no", "n", "0"):
            return False
        else:
            return None