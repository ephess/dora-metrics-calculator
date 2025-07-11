"""DORA metrics calculator with flexible calculation methods."""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

from ..logging import get_logger
from ..models import Commit, Deployment, PullRequest
from .quality import DataQualityValidator

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
    lead_time_for_changes: Optional[float]  # Hours (median)
    deployment_frequency: Optional[float]  # Deployments per day
    change_failure_rate: Optional[float]  # Ratio (0.0 to 1.0)
    mean_time_to_restore: Optional[float]  # Hours
    
    # Period this metric represents
    period_start: datetime
    period_end: datetime
    
    # Additional context
    lead_time_data_points: int = 0
    deployment_count: int = 0
    failed_deployment_count: int = 0
    mttr_data_points: int = 0
    
    # Statistical enhancements for lead time
    lead_time_p50: Optional[float] = None  # 50th percentile (median)
    lead_time_p90: Optional[float] = None  # 90th percentile
    lead_time_p95: Optional[float] = None  # 95th percentile
    lead_time_mean: Optional[float] = None  # Mean (shows impact of outliers)
    lead_time_std_dev: Optional[float] = None  # Standard deviation
    lead_time_min: Optional[float] = None  # Minimum (fastest)
    lead_time_max: Optional[float] = None  # Maximum (slowest - important!)
    
    # Statistical enhancements for MTTR
    mttr_p50: Optional[float] = None  # 50th percentile (median)
    mttr_p90: Optional[float] = None  # 90th percentile
    mttr_p95: Optional[float] = None  # 95th percentile
    mttr_mean: Optional[float] = None  # Mean
    mttr_std_dev: Optional[float] = None  # Standard deviation
    mttr_min: Optional[float] = None  # Fastest recovery
    mttr_max: Optional[float] = None  # Slowest recovery (critical!)
    
    # Configuration used
    config: Optional[MetricsConfig] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result = {
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
        }
        
        # Add lead time statistics if available
        if self.lead_time_data_points > 0:
            result["lead_time_statistics"] = {
                "p50": self.lead_time_p50,
                "p90": self.lead_time_p90,
                "p95": self.lead_time_p95,
                "mean": self.lead_time_mean,
                "std_dev": self.lead_time_std_dev,
                "min": self.lead_time_min,
                "max": self.lead_time_max,
            }
        
        # Add MTTR statistics if available
        if self.mttr_data_points > 0:
            result["mttr_statistics"] = {
                "p50": self.mttr_p50,
                "p90": self.mttr_p90,
                "p95": self.mttr_p95,
                "mean": self.mttr_mean,
                "std_dev": self.mttr_std_dev,
                "min": self.mttr_min,
                "max": self.mttr_max,
            }
        
        # Add config section if available
        if self.config:
            result["config"] = {
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
            }
        
        return result
        
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
        self.all_deployments: List[Tuple[datetime, Commit, Optional[Deployment]]] = []
        self.commits_ordered: List[Commit] = []
        
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
        
        # Keep ordered list of commits for finding ranges
        self.commits_ordered = sorted(commits, key=lambda c: c.committed_date)
        
        # Build complete deployment list for tracking previous deployments
        self.all_deployments = self._get_all_deployments_sorted()
        
    def _get_period_boundaries(
        self,
        start_date: datetime,
        end_date: datetime,
        period: Period
    ) -> List[Tuple[datetime, datetime]]:
        """Get list of period start/end dates."""
        # Map rolling windows to appropriate reporting periods
        if period == Period.ROLLING_7_DAYS:
            # 7-day rolling window: report daily
            period = Period.DAILY
        elif period == Period.ROLLING_30_DAYS:
            # 30-day rolling window: report weekly
            period = Period.WEEKLY
        elif period == Period.ROLLING_90_DAYS:
            # 90-day rolling window: report weekly
            period = Period.WEEKLY
            
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
        # Calculate lead time with statistics
        deployments = self._get_deployments_for_metric(
            period_start, period_end, config.lead_time
        )
        lead_time, lt_points, lt_stats = self._calculate_lead_time(
            deployments, period_start, period_end
        )
        
        # Calculate deployment frequency
        deploy_freq, deploy_count = self._calculate_metric(
            period_start, period_end, config.deployment_frequency, self._calculate_deployment_frequency
        )
        
        # Calculate failure rate
        failure_rate, failed_count = self._calculate_metric(
            period_start, period_end, config.change_failure_rate, self._calculate_change_failure_rate
        )
        
        # Calculate MTTR with statistics
        deployments = self._get_deployments_for_metric(
            period_start, period_end, config.mttr
        )
        mttr, mttr_points, mttr_stats = self._calculate_mttr(
            deployments, period_start, period_end
        )
        
        # Create metrics object
        metrics = DORAMetrics(
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
        
        # Add lead time statistics if available
        if lt_stats:
            metrics.lead_time_p50 = lt_stats.get('p50')
            metrics.lead_time_p90 = lt_stats.get('p90')
            metrics.lead_time_p95 = lt_stats.get('p95')
            metrics.lead_time_mean = lt_stats.get('mean')
            metrics.lead_time_std_dev = lt_stats.get('std_dev')
            metrics.lead_time_min = lt_stats.get('min')
            metrics.lead_time_max = lt_stats.get('max')
        
        # Add MTTR statistics if available
        if mttr_stats:
            metrics.mttr_p50 = mttr_stats.get('p50')
            metrics.mttr_p90 = mttr_stats.get('p90')
            metrics.mttr_p95 = mttr_stats.get('p95')
            metrics.mttr_mean = mttr_stats.get('mean')
            metrics.mttr_std_dev = mttr_stats.get('std_dev')
            metrics.mttr_min = mttr_stats.get('min')
            metrics.mttr_max = mttr_stats.get('max')
        
        return metrics
        
    def _get_deployments_for_metric(
        self,
        period_start: datetime,
        period_end: datetime,
        metric_config: MetricConfig
    ) -> List[Tuple[datetime, Commit, Optional[Deployment]]]:
        """Get deployments for a specific metric based on its configuration."""
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
        return self._get_deployments_in_period(data_start, period_end)
    
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
        # Get deployments for this metric
        deployments = self._get_deployments_for_metric(period_start, period_end, metric_config)
        
        # Calculate the metric
        return calculation_func(deployments, period_start, period_end)
        
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
        
    def _get_all_deployments_sorted(self) -> List[Tuple[datetime, Commit, Optional[Deployment]]]:
        """Get all deployments sorted by time (for finding previous deployments)."""
        deployments = []
        
        # GitHub deployments
        for deployment in self.deployments_by_tag.values():
            deploy_time = deployment.published_at or deployment.created_at
            if deployment.commit_sha in self.commits_by_sha:
                commit = self.commits_by_sha[deployment.commit_sha]
                deployments.append((deploy_time, commit, deployment))
                
        # Manual deployments from commits
        for commit in self.commits_by_sha.values():
            if getattr(commit, "is_manual_deployment", None):
                deploy_time = getattr(commit, "manual_deployment_timestamp", commit.committed_date)
                deployments.append((deploy_time, commit, None))
                
        # Sort by deployment time
        deployments.sort(key=lambda x: x[0])
        return deployments
        
    def _calculate_lead_time(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional[float], int, Dict[str, Optional[float]]]:
        """
        Calculate lead time for changes in hours with full statistics.
        
        Lead time is measured from commit authored date to deployment time.
        
        Returns:
            Tuple of (median_lead_time_hours, number_of_data_points, statistics_dict)
        """
        if not deployments:
            return None, 0, {}
            
        lead_times = []
        
        for deploy_time, deploy_commit, deployment in deployments:
            # Get all commits in this deployment
            commits_in_deployment = self._get_commits_in_deployment(
                deployment if deployment else deploy_commit,
                deploy_time
            )
            
            for commit in commits_in_deployment:
                lead_time = (deploy_time - commit.authored_date).total_seconds() / 3600
                # Only count positive lead times (commit before deployment)
                if lead_time >= 0:
                    lead_times.append(lead_time)
                    
        if not lead_times:
            return None, 0, {}
            
        # Calculate comprehensive statistics
        lead_times_array = np.array(lead_times)
        statistics = {
            'p50': np.percentile(lead_times_array, 50),
            'p90': np.percentile(lead_times_array, 90),
            'p95': np.percentile(lead_times_array, 95),
            'mean': np.mean(lead_times_array),
            'std_dev': np.std(lead_times_array) if len(lead_times) > 1 else 0.0,
            'min': np.min(lead_times_array),
            'max': np.max(lead_times_array),
        }
            
        return statistics['p50'], len(lead_times), statistics
        
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
        
        return failed / len(deployments), failed
        
    def _calculate_mttr(
        self,
        deployments: List[Tuple[datetime, Commit, Optional[Deployment]]],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional[float], int, Dict[str, Optional[float]]]:
        """
        Calculate mean time to restore in hours with full statistics.
        
        MTTR = time from failure to resolution
        
        Returns:
            Tuple of (median_restore_time_hours, number_of_restorations, statistics_dict)
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
            return None, 0, {}
            
        # Calculate comprehensive statistics
        restore_times_array = np.array(restore_times)
        statistics = {
            'p50': np.percentile(restore_times_array, 50),
            'p90': np.percentile(restore_times_array, 90),
            'p95': np.percentile(restore_times_array, 95),
            'mean': np.mean(restore_times_array),
            'std_dev': np.std(restore_times_array) if len(restore_times) > 1 else 0.0,
            'min': np.min(restore_times_array),
            'max': np.max(restore_times_array),
        }
            
        return statistics['p50'], len(restore_times), statistics
        
    def _get_commits_in_deployment(
        self,
        deployment: Union[Deployment, Commit],
        deploy_time: Optional[datetime] = None
    ) -> List[Commit]:
        """
        Get all commits included in a deployment.
        
        This includes all commits since the previous deployment.
        """
        commits = []
        
        # Get deployment commit
        if isinstance(deployment, Deployment):
            if deployment.commit_sha not in self.commits_by_sha:
                return []
            deployment_commit = self.commits_by_sha[deployment.commit_sha]
            if not deploy_time:
                deploy_time = deployment.published_at or deployment.created_at
        else:
            # Manual deployment
            deployment_commit = deployment
            if not deploy_time:
                deploy_time = getattr(deployment, "manual_deployment_timestamp", deployment.committed_date)
        
        # Find previous deployment
        prev_deployment = None
        prev_deploy_time = None
        
        for d_time, d_commit, d_deployment in self.all_deployments:
            if d_time < deploy_time:
                prev_deployment = d_deployment if d_deployment else d_commit
                prev_deploy_time = d_time
            else:
                break
        
        # Get all commits between previous deployment and this one
        if prev_deployment:
            # Find commits after previous deployment and up to current deployment time
            for commit in self.commits_ordered:
                # Include commits authored after the previous deployment
                # and before or at the deployment time
                if (commit.authored_date > prev_deploy_time and 
                    commit.authored_date <= deploy_time):
                    commits.append(commit)
        else:
            # First deployment - include all commits up to deployment time
            for commit in self.commits_ordered:
                if commit.authored_date <= deploy_time:
                    commits.append(commit)
        
        return commits
        
    def _is_deployment_failed(self, deployment: Union[Deployment, Commit]) -> bool:
        """Check if a deployment failed."""
        if isinstance(deployment, Deployment):
            return getattr(deployment, "deployment_failed", False) is True
        else:
            # Manual deployment from commit
            return getattr(deployment, "manual_deployment_failed", False) is True
    
    def calculate_daily_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate daily DORA metrics."""
        # Default to last 30 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=30)
        
        config = MetricsConfig(reporting_period=Period.DAILY)
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by date string
        result = {}
        for m in metrics_list:
            # Format period as YYYY-MM-DD for daily
            period_key = m.period_start.strftime("%Y-%m-%d")
            result[period_key] = m
        return result
    
    def calculate_weekly_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate weekly DORA metrics."""
        # Default to last 90 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=90)
        
        config = MetricsConfig(reporting_period=Period.WEEKLY)
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by week string
        result = {}
        for m in metrics_list:
            # Format period as Week YYYY-WW
            week_num = m.period_start.isocalendar()[1]
            year = m.period_start.year
            period_key = f"{year}-W{week_num:02d}"
            result[period_key] = m
        return result
    
    def calculate_monthly_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate monthly DORA metrics."""
        # Default to last 365 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=365)
        
        config = MetricsConfig(reporting_period=Period.MONTHLY)
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by month string  
        result = {}
        for m in metrics_list:
            # Format period as YYYY-MM
            period_key = m.period_start.strftime("%Y-%m")
            result[period_key] = m
        return result
    
    def calculate_quarterly_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate quarterly DORA metrics."""
        # Default to last 2 years if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=730)
        
        config = MetricsConfig(reporting_period=Period.QUARTERLY)
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by quarter string  
        result = {}
        for m in metrics_list:
            # Format period as YYYY-Q#
            quarter = (m.period_start.month - 1) // 3 + 1
            period_key = f"{m.period_start.year}-Q{quarter}"
            result[period_key] = m
        return result
    
    def calculate_yearly_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate yearly DORA metrics."""
        # Default to last 5 years if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=1825)  # 5 years
        
        config = MetricsConfig(reporting_period=Period.YEARLY)
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by year string  
        result = {}
        for m in metrics_list:
            # Format period as YYYY
            period_key = str(m.period_start.year)
            result[period_key] = m
        return result
    
    def calculate_rolling_7_days_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate rolling 7-day DORA metrics."""
        # Default to last 30 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=30)
        
        # Configure with rolling window for all metrics
        config = MetricsConfig(
            lead_time=MetricConfig(Period.ROLLING_7_DAYS, CalculationMethod.ROLLING_WINDOW),
            deployment_frequency=MetricConfig(Period.ROLLING_7_DAYS, CalculationMethod.ROLLING_WINDOW),
            change_failure_rate=MetricConfig(Period.ROLLING_7_DAYS, CalculationMethod.ROLLING_WINDOW),
            mttr=MetricConfig(Period.ROLLING_7_DAYS, CalculationMethod.ROLLING_WINDOW),
            reporting_period=Period.ROLLING_7_DAYS
        )
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by date string  
        result = {}
        for m in metrics_list:
            # Format period as YYYY-MM-DD (daily)
            period_key = m.period_start.strftime("%Y-%m-%d")
            result[period_key] = m
        return result
    
    def calculate_rolling_30_days_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate rolling 30-day DORA metrics."""
        # Default to last 90 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=90)
        
        # Configure with rolling window for all metrics
        config = MetricsConfig(
            lead_time=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            deployment_frequency=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            change_failure_rate=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            mttr=MetricConfig(Period.ROLLING_30_DAYS, CalculationMethod.ROLLING_WINDOW),
            reporting_period=Period.ROLLING_30_DAYS
        )
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by week string  
        result = {}
        for m in metrics_list:
            # Format period as Week YYYY-WW (weekly)
            week_num = m.period_start.isocalendar()[1]
            year = m.period_start.year
            period_key = f"{year}-W{week_num:02d}"
            result[period_key] = m
        return result
    
    def calculate_rolling_90_days_metrics(
        self,
        commits: List[Commit],
        deployments: List[Deployment],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> Dict[str, DORAMetrics]:
        """Calculate rolling 90-day DORA metrics."""
        # Default to last 180 days if no range specified
        if until is None:
            until = datetime.now(timezone.utc)
        if since is None:
            since = until - timedelta(days=180)
        
        # Configure with rolling window for all metrics
        config = MetricsConfig(
            lead_time=MetricConfig(Period.ROLLING_90_DAYS, CalculationMethod.ROLLING_WINDOW),
            deployment_frequency=MetricConfig(Period.ROLLING_90_DAYS, CalculationMethod.ROLLING_WINDOW),
            change_failure_rate=MetricConfig(Period.ROLLING_90_DAYS, CalculationMethod.ROLLING_WINDOW),
            mttr=MetricConfig(Period.ROLLING_90_DAYS, CalculationMethod.ROLLING_WINDOW),
            reporting_period=Period.ROLLING_90_DAYS
        )
        metrics_list = self.calculate(commits, [], deployments, since, until, config)
        
        # Convert to dict keyed by week string  
        result = {}
        for m in metrics_list:
            # Format period as Week YYYY-WW (weekly)
            week_num = m.period_start.isocalendar()[1]
            year = m.period_start.year
            period_key = f"{year}-W{week_num:02d}"
            result[period_key] = m
        return result