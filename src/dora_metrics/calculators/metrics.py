"""DORA metrics calculator with flexible calculation methods."""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

from ..logging import get_logger
from ..models import Commit, Deployment, PullRequest

logger = get_logger(__name__)


class Period(Enum):
    """Time period for metric aggregation."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    ROLLING_7_DAYS = "rolling_7_days"
    ROLLING_30_DAYS = "rolling_30_days"
    ROLLING_90_DAYS = "rolling_90_days"


class CalculationMethod(Enum):
    """Method for calculating metrics."""
    PERIOD_BASED = "period_based"  # Only data within the period
    ROLLING_WINDOW = "rolling_window"  # Look back N days from period end


@dataclass
class MetricConfig:
    """Configuration for how to calculate a specific metric."""
    period: Period
    method: CalculationMethod = CalculationMethod.PERIOD_BASED
    
    def get_window_days(self) -> int:
        """Get the number of days for rolling window periods."""
        if self.period == Period.ROLLING_7_DAYS:
            return 7
        elif self.period == Period.ROLLING_30_DAYS:
            return 30
        elif self.period == Period.ROLLING_90_DAYS:
            return 90
        else:
            return 0


@dataclass
class MetricsConfig:
    """Configuration for all DORA metrics calculations."""
    lead_time: MetricConfig = field(default_factory=lambda: MetricConfig(Period.WEEKLY))
    deployment_frequency: MetricConfig = field(default_factory=lambda: MetricConfig(Period.DAILY))
    change_failure_rate: MetricConfig = field(default_factory=lambda: MetricConfig(Period.MONTHLY))
    mttr: MetricConfig = field(default_factory=lambda: MetricConfig(Period.MONTHLY))
    
    # The primary period for reporting (determines the x-axis of charts)
    reporting_period: Period = Period.WEEKLY
    
    @classmethod
    def daily_all(cls) -> "MetricsConfig":
        """All metrics calculated daily."""
        return cls(
            lead_time=MetricConfig(Period.DAILY),
            deployment_frequency=MetricConfig(Period.DAILY),
            change_failure_rate=MetricConfig(Period.DAILY),
            mttr=MetricConfig(Period.DAILY),
            reporting_period=Period.DAILY,
        )
        
    @classmethod
    def recommended(cls) -> "MetricsConfig":
        """Recommended configuration for most teams."""
        return cls(
            lead_time=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            deployment_frequency=MetricConfig(Period.DAILY),
            change_failure_rate=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            mttr=MetricConfig(Period.ROLLING_90_DAYS, CalculationMethod.ROLLING_WINDOW),
            reporting_period=Period.WEEKLY,
        )
        
    @classmethod
    def quarterly_view(cls) -> "MetricsConfig":
        """Configuration for quarterly reporting."""
        return cls(
            lead_time=MetricConfig(Period.QUARTERLY),
            deployment_frequency=MetricConfig(Period.QUARTERLY),
            change_failure_rate=MetricConfig(Period.QUARTERLY),
            mttr=MetricConfig(Period.QUARTERLY),
            reporting_period=Period.QUARTERLY,
        )


@dataclass
class DORAMetrics:
    """Container for DORA metrics results."""
    lead_time_for_changes: Optional[float]  # Hours
    deployment_frequency: Optional[float]  # Deployments per day
    change_failure_rate: Optional[float]  # Percentage
    mean_time_to_restore: Optional[float]  # Hours
    
    # Period this metric represents
    period_start: datetime
    period_end: datetime
    
    # Additional context
    lead_time_data_points: int = 0
    deployment_count: int = 0
    failed_deployment_count: int = 0
    mttr_data_points: int = 0
    
    # Configuration used
    config: Optional[MetricsConfig] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "metrics": {
                "lead_time_for_changes_hours": self.lead_time_for_changes,
                "deployment_frequency_per_day": self.deployment_frequency,
                "change_failure_rate_percent": self.change_failure_rate,
                "mean_time_to_restore_hours": self.mean_time_to_restore,
            },
            "context": {
                "lead_time_data_points": self.lead_time_data_points,
                "deployment_count": self.deployment_count,
                "failed_deployment_count": self.failed_deployment_count,
                "mttr_data_points": self.mttr_data_points,
            },
            "config": {
                "lead_time": {
                    "period": self.config.lead_time.period.value,
                    "method": self.config.lead_time.method.value,
                },
                "deployment_frequency": {
                    "period": self.config.deployment_frequency.period.value,
                    "method": self.config.deployment_frequency.method.value,
                },
                "change_failure_rate": {
                    "period": self.config.change_failure_rate.period.value,
                    "method": self.config.change_failure_rate.method.value,
                },
                "mttr": {
                    "period": self.config.mttr.period.value,
                    "method": self.config.mttr.method.value,
                },
                "reporting_period": self.config.reporting_period.value,
            } if self.config else None,
        }
        
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class MetricsCalculator:
    """Calculates DORA metrics from associated data."""
    
    def __init__(self):
        """Initialize the calculator."""
        self.commits_by_sha: Dict[str, Commit] = {}
        self.prs_by_number: Dict[int, PullRequest] = {}
        self.deployments_by_tag: Dict[str, Deployment] = {}
        
    def calculate(
        self,
        commits: List[Commit],
        pull_requests: List[PullRequest],
        deployments: List[Deployment],
        start_date: datetime,
        end_date: datetime,
        config: Optional[MetricsConfig] = None
    ) -> List[DORAMetrics]:
        """
        Calculate DORA metrics for the given time period.
        
        Args:
            commits: List of commits with associations
            pull_requests: List of pull requests
            deployments: List of deployments
            start_date: Start of analysis period
            end_date: End of analysis period
            config: Configuration for metric calculations
            
        Returns:
            List of DORAMetrics for each reporting period
        """
        if config is None:
            config = MetricsConfig.recommended()
            
        logger.info(f"Calculating DORA metrics from {start_date} to {end_date}")
        logger.info(f"Using config: reporting_period={config.reporting_period.value}")
        
        # Build lookup structures
        self._build_lookups(commits, pull_requests, deployments)
        
        # Get reporting period boundaries
        periods = self._get_period_boundaries(start_date, end_date, config.reporting_period)
        
        # Calculate metrics for each reporting period
        results = []
        for period_start, period_end in periods:
            metrics = self._calculate_period_metrics(period_start, period_end, config)
            results.append(metrics)
            
        return results
        
    def _build_lookups(
        self,
        commits: List[Commit],
        pull_requests: List[PullRequest],
        deployments: List[Deployment]
    ) -> None:
        """Build lookup dictionaries."""
        self.commits_by_sha = {c.sha: c for c in commits}
        self.prs_by_number = {pr.number: pr for pr in pull_requests}
        self.deployments_by_tag = {d.tag_name: d for d in deployments}
        
    def _get_period_boundaries(
        self,
        start_date: datetime,
        end_date: datetime,
        period: Period
    ) -> List[Tuple[datetime, datetime]]:
        """Get list of period start/end dates."""
        if period in (Period.ROLLING_7_DAYS, Period.ROLLING_30_DAYS, Period.ROLLING_90_DAYS):
            # For rolling windows, still create daily periods for reporting
            period = Period.DAILY
            
        periods = []
        current = start_date
        
        while current < end_date:
            if period == Period.DAILY:
                period_end = current + timedelta(days=1)
            elif period == Period.WEEKLY:
                # Week starts on Monday
                days_until_sunday = 6 - current.weekday()
                period_end = current + timedelta(days=days_until_sunday + 1)
            elif period == Period.MONTHLY:
                # Find first day of next month
                if current.month == 12:
                    period_end = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    period_end = current.replace(month=current.month + 1, day=1)
            elif period == Period.QUARTERLY:
                # Find first day of next quarter
                current_quarter = (current.month - 1) // 3
                if current_quarter == 3:  # Q4
                    period_end = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_quarter_month = (current_quarter + 1) * 3 + 1
                    period_end = current.replace(month=next_quarter_month, day=1)
            elif period == Period.YEARLY:
                # Find first day of next year
                period_end = current.replace(year=current.year + 1, month=1, day=1)
            else:
                raise ValueError(f"Unknown period type: {period}")
                    
            # Don't exceed end_date
            period_end = min(period_end, end_date)
            periods.append((current, period_end))
            current = period_end
            
        return periods
        
    def _calculate_period_metrics(
        self,
        period_start: datetime,
        period_end: datetime,
        config: MetricsConfig
    ) -> DORAMetrics:
        """Calculate metrics for a single reporting period using configured methods."""
        # Calculate each metric with its own configuration
        lead_time, lt_points = self._calculate_metric(
            period_start, period_end, config.lead_time, self._calculate_lead_time
        )
        
        deploy_freq, deploy_count = self._calculate_metric(
            period_start, period_end, config.deployment_frequency, self._calculate_deployment_frequency
        )
        
        failure_rate, failed_count = self._calculate_metric(
            period_start, period_end, config.change_failure_rate, self._calculate_change_failure_rate
        )
        
        mttr, mttr_points = self._calculate_metric(
            period_start, period_end, config.mttr, self._calculate_mttr
        )
        
        return DORAMetrics(
            lead_time_for_changes=lead_time,
            deployment_frequency=deploy_freq,
            change_failure_rate=failure_rate,
            mean_time_to_restore=mttr,
            period_start=period_start,
            period_end=period_end,
            lead_time_data_points=lt_points,
            deployment_count=int(deploy_count) if deploy_count else 0,
            failed_deployment_count=int(failed_count) if failed_count else 0,
            mttr_data_points=mttr_points,
            config=config,
        )
        
    def _calculate_metric(
        self,
        period_start: datetime,
        period_end: datetime,
        metric_config: MetricConfig,
        calculation_func
    ) -> Tuple[Optional[float], int]:
        """
        Calculate a single metric using its configuration.
        
        Returns:
            Tuple of (metric_value, data_point_count)
        """
        # Determine the data window based on configuration
        if metric_config.method == CalculationMethod.ROLLING_WINDOW:
            window_days = metric_config.get_window_days()
            if window_days > 0:
                data_start = period_end - timedelta(days=window_days)
            else:
                # Non-rolling period used with rolling method - use period as window
                data_start = period_start
        else:
            # Period-based calculation
            data_start = period_start
            
        # Get deployments in the data window
        deployments = self._get_deployments_in_period(data_start, period_end)
        
        # Calculate the metric
        return calculation_func(deployments, data_start, period_end)
        
    def _get_deployments_in_period(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Tuple[datetime, Commit, Optional[Deployment]]]:
        """
        Get all deployments (GitHub and manual) in the period.
        
        Returns:
            List of (deployment_time, commit, deployment) tuples
        """
        deployments = []
        
        # GitHub deployments
        for deployment in self.deployments_by_tag.values():
            deploy_time = deployment.published_at or deployment.created_at
            if start_date <= deploy_time < end_date:
                if deployment.commit_sha in self.commits_by_sha:
                    commit = self.commits_by_sha[deployment.commit_sha]
                    deployments.append((deploy_time, commit, deployment))
                    
        # Manual deployments from commits
        for commit in self.commits_by_sha.values():
            if getattr(commit, "is_manual_deployment", None):
                deploy_time = getattr(commit, "manual_deployment_timestamp", commit.committed_date)
                if start_date <= deploy_time < end_date:
                    deployments.append((deploy_time, commit, None))
                    
        # Sort by deployment time
        deployments.sort(key=lambda x: x[0])
        return deployments
        
    def _calculate_lead_time(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional[float], int]:
        """
        Calculate lead time for changes in hours.
        
        Lead time is measured from commit authored date to deployment time.
        
        Returns:
            Tuple of (median_lead_time_hours, number_of_data_points)
        """
        if not deployments:
            return None, 0
            
        lead_times = []
        
        for deploy_time, deploy_commit, deployment in deployments:
            # Get all commits in this deployment
            commits_in_deployment = self._get_commits_in_deployment(
                deployment if deployment else deploy_commit
            )
            
            for commit in commits_in_deployment:
                lead_time = (deploy_time - commit.authored_date).total_seconds() / 3600
                # Only count positive lead times (commit before deployment)
                if lead_time >= 0:
                    lead_times.append(lead_time)
                    
        if not lead_times:
            return None, 0
            
        # Return median lead time
        lead_times.sort()
        n = len(lead_times)
        if n % 2 == 0:
            median = (lead_times[n//2 - 1] + lead_times[n//2]) / 2
        else:
            median = lead_times[n//2]
            
        return median, n
        
    def _calculate_deployment_frequency(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[float, int]:
        """
        Calculate deployment frequency (deployments per day).
        
        Only counts successful deployments.
        
        Returns:
            Tuple of (deployments_per_day, total_deployment_count)
        """
        # Filter out failed deployments
        successful_deployments = [
            d for d in deployments
            if not self._is_deployment_failed(d[2] if d[2] else d[1])
        ]
        
        # Calculate days in period
        days = (end_date - start_date).total_seconds() / 86400
        
        if days == 0:
            return 0.0, 0
            
        return len(successful_deployments) / days, len(successful_deployments)
        
    def _calculate_change_failure_rate(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional[float], int]:
        """
        Calculate change failure rate as percentage.
        
        Returns:
            Tuple of (failure_rate_percent, failed_deployment_count)
        """
        if not deployments:
            return None, 0
            
        failed = sum(
            1 for d in deployments
            if self._is_deployment_failed(d[2] if d[2] else d[1])
        )
        
        return (failed / len(deployments)) * 100, failed
        
    def _calculate_mttr(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional[float], int]:
        """
        Calculate mean time to restore in hours.
        
        MTTR = average time from failure to resolution
        
        Returns:
            Tuple of (mean_restore_time_hours, number_of_restorations)
        """
        restore_times = []
        
        for deploy_time, commit, deployment in deployments:
            if deployment and self._is_deployment_failed(deployment):
                # GitHub deployment failure
                if hasattr(deployment, "failure_resolved_at") and deployment.failure_resolved_at:
                    restore_time = (deployment.failure_resolved_at - deploy_time).total_seconds() / 3600
                    restore_times.append(restore_time)
            elif self._is_deployment_failed(commit):
                # Manual deployment failure
                # For manual deployments, we need to find the next successful deployment
                # This is a limitation - users should provide failure_resolved_at in CSV
                pass
                
        if not restore_times:
            return None, 0
            
        return sum(restore_times) / len(restore_times), len(restore_times)
        
    def _get_commits_in_deployment(
        self,
        deployment: Union[Deployment, Commit]
    ) -> List[Commit]:
        """
        Get all commits included in a deployment.
        
        Note: This is a simplified implementation. In a full system, you would
        track commit ranges between deployments to know exactly which commits
        are included in each deployment.
        """
        commits = []
        
        if isinstance(deployment, Deployment):
            # For GitHub deployments, we currently only return the deployment commit
            # A full implementation would track all commits since the last deployment
            if deployment.commit_sha in self.commits_by_sha:
                commits.append(self.commits_by_sha[deployment.commit_sha])
        else:
            # For manual deployments, just the commit itself
            commits.append(deployment)
            
        return commits
        
    def _is_deployment_failed(self, deployment: Union[Deployment, Commit]) -> bool:
        """Check if a deployment failed."""
        if isinstance(deployment, Deployment):
            return getattr(deployment, "deployment_failed", False) is True
        else:
            # Manual deployment from commit
            return getattr(deployment, "manual_deployment_failed", False) is True