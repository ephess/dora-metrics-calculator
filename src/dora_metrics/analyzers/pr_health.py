"""PR health analyzer for tracking pull request lifecycle and flow efficiency."""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

from ..models import PullRequest, PRState


class PRHealthStatus(Enum):
    """PR health status based on activity flow."""
    ACTIVE = "active"      # Last activity within 7 days
    STALE = "stale"        # No activity for 7-30 days
    ABANDONED = "abandoned" # No activity for 30+ days


class PRSize(Enum):
    """PR size categories based on lines changed."""
    SMALL = "small"        # < 100 lines
    MEDIUM = "medium"      # 100-500 lines
    LARGE = "large"        # > 500 lines


@dataclass
class PRHealthMetrics:
    """Health metrics for a single PR."""
    pr_number: int
    title: str
    author: str
    status: PRHealthStatus
    size: PRSize
    age_days: int
    days_since_activity: int
    created_at: datetime
    updated_at: datetime
    additions: int
    deletions: int
    commits_count: int
    
    @property
    def total_lines_changed(self) -> int:
        """Total lines changed in the PR."""
        return self.additions + self.deletions


@dataclass
class PRHealthReport:
    """Overall PR health report for a repository."""
    
    # Summary counts
    total_open_prs: int = 0
    active_count: int = 0
    stale_count: int = 0
    abandoned_count: int = 0
    
    # Size distribution
    small_count: int = 0
    medium_count: int = 0
    large_count: int = 0
    
    # Detailed lists
    active_prs: List[PRHealthMetrics] = field(default_factory=list)
    stale_prs: List[PRHealthMetrics] = field(default_factory=list)
    abandoned_prs: List[PRHealthMetrics] = field(default_factory=list)
    
    # Age statistics
    median_age_days: Optional[float] = None
    oldest_pr_age_days: Optional[int] = None
    
    # Risk metrics
    total_stale_days: int = 0  # Sum of days all stale PRs have been waiting
    total_abandoned_days: int = 0  # Sum of days all abandoned PRs have been waiting
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    
    def get_summary(self) -> str:
        """Get a brief summary of PR health."""
        lines = []
        lines.append(f"Total Open PRs: {self.total_open_prs}")
        lines.append(f"  Active: {self.active_count} ({self.active_count/self.total_open_prs*100:.0f}%)")
        lines.append(f"  Stale: {self.stale_count} ({self.stale_count/self.total_open_prs*100:.0f}%)")
        lines.append(f"  Abandoned: {self.abandoned_count} ({self.abandoned_count/self.total_open_prs*100:.0f}%)")
        
        if self.stale_count > 0:
            lines.append(f"\n⚠️  {self.stale_count} PRs need attention (stale for 7-30 days)")
        
        if self.abandoned_count > 0:
            lines.append(f"❌ {self.abandoned_count} PRs should be closed or revived (abandoned 30+ days)")
            
        return "\n".join(lines)
    
    def get_detailed_report(self) -> str:
        """Get a detailed PR health report."""
        lines = []
        lines.append("=" * 60)
        lines.append("PR HEALTH REPORT")
        lines.append("=" * 60)
        lines.append("")
        
        # Summary
        lines.append("SUMMARY")
        lines.append("-" * 20)
        lines.append(f"Total Open PRs: {self.total_open_prs}")
        lines.append(f"  Active: {self.active_count}")
        lines.append(f"  Stale: {self.stale_count}")
        lines.append(f"  Abandoned: {self.abandoned_count}")
        lines.append("")
        
        # Size distribution
        lines.append("SIZE DISTRIBUTION")
        lines.append("-" * 20)
        lines.append(f"  Small (<100 lines): {self.small_count}")
        lines.append(f"  Medium (100-500 lines): {self.medium_count}")
        lines.append(f"  Large (>500 lines): {self.large_count}")
        lines.append("")
        
        # Age statistics
        if self.median_age_days is not None:
            lines.append("AGE STATISTICS")
            lines.append("-" * 20)
            lines.append(f"  Median age: {self.median_age_days:.0f} days")
            lines.append(f"  Oldest PR: {self.oldest_pr_age_days} days")
            lines.append("")
        
        # Stale PRs
        if self.stale_prs:
            lines.append("STALE PRS (need attention)")
            lines.append("-" * 20)
            for pr in sorted(self.stale_prs, key=lambda x: x.days_since_activity, reverse=True)[:10]:
                lines.append(f"  • PR #{pr.pr_number}: {pr.title[:50]}...")
                lines.append(f"    Author: {pr.author}, Size: {pr.size.value}, "
                           f"Inactive: {pr.days_since_activity} days")
            if len(self.stale_prs) > 10:
                lines.append(f"  ... and {len(self.stale_prs) - 10} more")
            lines.append("")
        
        # Abandoned PRs
        if self.abandoned_prs:
            lines.append("ABANDONED PRS (close or revive)")
            lines.append("-" * 20)
            for pr in sorted(self.abandoned_prs, key=lambda x: x.age_days, reverse=True)[:10]:
                lines.append(f"  • PR #{pr.pr_number}: {pr.title[:50]}...")
                lines.append(f"    Author: {pr.author}, Age: {pr.age_days} days, "
                           f"Size: {pr.size.value}")
            if len(self.abandoned_prs) > 10:
                lines.append(f"  ... and {len(self.abandoned_prs) - 10} more")
            lines.append("")
        
        # Recommendations
        if self.recommendations:
            lines.append("RECOMMENDATIONS")
            lines.append("-" * 20)
            for i, rec in enumerate(self.recommendations, 1):
                lines.append(f"{i}. {rec}")
            lines.append("")
            
        return "\n".join(lines)


class PRHealthAnalyzer:
    """Analyzes PR health and lifecycle efficiency."""
    
    def __init__(self, reference_time: Optional[datetime] = None):
        """
        Initialize the analyzer.
        
        Args:
            reference_time: Time to use as "now" for testing (defaults to current time)
        """
        self.reference_time = reference_time or datetime.now(timezone.utc)
    
    def analyze(self, pull_requests: List[PullRequest]) -> PRHealthReport:
        """
        Analyze PR health for a list of pull requests.
        
        Args:
            pull_requests: List of pull requests to analyze
            
        Returns:
            PRHealthReport with categorized PRs and recommendations
        """
        report = PRHealthReport()
        
        # Filter to open PRs only
        open_prs = [pr for pr in pull_requests if pr.state == PRState.OPEN]
        report.total_open_prs = len(open_prs)
        
        if not open_prs:
            return report
        
        # Analyze each PR
        pr_metrics = []
        for pr in open_prs:
            metrics = self._analyze_single_pr(pr)
            pr_metrics.append(metrics)
            
            # Categorize by status
            if metrics.status == PRHealthStatus.ACTIVE:
                report.active_prs.append(metrics)
                report.active_count += 1
            elif metrics.status == PRHealthStatus.STALE:
                report.stale_prs.append(metrics)
                report.stale_count += 1
                report.total_stale_days += metrics.days_since_activity
            else:  # ABANDONED
                report.abandoned_prs.append(metrics)
                report.abandoned_count += 1
                report.total_abandoned_days += metrics.days_since_activity
            
            # Categorize by size
            if metrics.size == PRSize.SMALL:
                report.small_count += 1
            elif metrics.size == PRSize.MEDIUM:
                report.medium_count += 1
            else:  # LARGE
                report.large_count += 1
        
        # Calculate age statistics
        ages = [m.age_days for m in pr_metrics]
        if ages:
            ages.sort()
            report.median_age_days = ages[len(ages) // 2]
            report.oldest_pr_age_days = max(ages)
        
        # Generate recommendations
        self._generate_recommendations(report)
        
        return report
    
    def _analyze_single_pr(self, pr: PullRequest) -> PRHealthMetrics:
        """Analyze health metrics for a single PR."""
        # Calculate age and activity
        age = self.reference_time - pr.created_at
        age_days = age.days
        
        last_activity = pr.updated_at or pr.created_at
        days_since_activity = (self.reference_time - last_activity).days
        
        # Determine status
        if days_since_activity < 7:
            status = PRHealthStatus.ACTIVE
        elif days_since_activity < 30:
            status = PRHealthStatus.STALE
        else:
            status = PRHealthStatus.ABANDONED
        
        # Determine size
        total_lines = pr.additions + pr.deletions
        if total_lines < 100:
            size = PRSize.SMALL
        elif total_lines <= 500:
            size = PRSize.MEDIUM
        else:
            size = PRSize.LARGE
        
        return PRHealthMetrics(
            pr_number=pr.number,
            title=pr.title,
            author=pr.author,
            status=status,
            size=size,
            age_days=age_days,
            days_since_activity=days_since_activity,
            created_at=pr.created_at,
            updated_at=pr.updated_at,
            additions=pr.additions,
            deletions=pr.deletions,
            commits_count=len(pr.commits)
        )
    
    def _generate_recommendations(self, report: PRHealthReport) -> None:
        """Generate actionable recommendations based on PR health."""
        # High abandoned count
        if report.abandoned_count > 5:
            report.recommendations.append(
                f"Close or archive {report.abandoned_count} abandoned PRs to reduce clutter"
            )
        
        # High stale percentage
        if report.stale_count > 0 and report.stale_count / report.total_open_prs > 0.3:
            report.recommendations.append(
                f"{report.stale_count/report.total_open_prs:.0%} of PRs are stale - "
                "schedule regular PR review sessions"
            )
        
        # Large PRs
        large_stale = [pr for pr in report.stale_prs if pr.size == PRSize.LARGE]
        large_abandoned = [pr for pr in report.abandoned_prs if pr.size == PRSize.LARGE]
        if len(large_stale) + len(large_abandoned) > 3:
            report.recommendations.append(
                "Break down large PRs into smaller chunks for easier review"
            )
        
        # Old PRs
        if report.oldest_pr_age_days and report.oldest_pr_age_days > 90:
            report.recommendations.append(
                f"Oldest PR is {report.oldest_pr_age_days} days old - "
                "consider setting WIP limits"
            )
        
        # Author concentration
        stale_authors = defaultdict(int)
        for pr in report.stale_prs + report.abandoned_prs:
            stale_authors[pr.author] += 1
        
        if stale_authors:
            top_author = max(stale_authors.items(), key=lambda x: x[1])
            if top_author[1] > 3:
                report.recommendations.append(
                    f"{top_author[0]} has {top_author[1]} stale/abandoned PRs - "
                    "check if they need help"
                )