"""Unit tests for metrics calculator."""

from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.calculators.metrics import (
    CalculationMethod,
    DORAMetrics,
    MetricConfig,
    MetricsCalculator,
    MetricsConfig,
    Period,
)
from dora_metrics.models import Commit, Deployment, PullRequest


@pytest.mark.unit
class TestMetricsCalculator:
    """Test DORA metrics calculations."""
    
    @pytest.fixture
    def calculator(self):
        """Create a metrics calculator."""
        return MetricsCalculator()
        
    @pytest.fixture
    def sample_commits(self):
        """Create sample commits for testing."""
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        return [
            Commit(
                sha=f"commit{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(days=i),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(days=i),
                message=f"Commit {i}",
                files_changed=[f"file{i}.py"],
                additions=10,
                deletions=5,
                deployment_tag="v1.0.0" if i == 2 else None,
            )
            for i in range(5)
        ]
        
    @pytest.fixture
    def sample_manual_deployments(self):
        """Create commits with manual deployment annotations."""
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        commits = []
        
        # Successful deployment
        commit1 = Commit(
            sha="manual1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=5),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=5),
            message="Manual deployment 1",
            files_changed=["deploy.py"],
            additions=50,
            deletions=10,
        )
        commit1.is_manual_deployment = True
        commit1.manual_deployment_timestamp = base_date + timedelta(days=6)
        commit1.manual_deployment_failed = False
        commits.append(commit1)
        
        # Failed deployment
        commit2 = Commit(
            sha="manual2",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=10),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=10),
            message="Manual deployment 2",
            files_changed=["app.py"],
            additions=20,
            deletions=5,
        )
        commit2.is_manual_deployment = True
        commit2.manual_deployment_timestamp = base_date + timedelta(days=11)
        commit2.manual_deployment_failed = True
        commits.append(commit2)
        
        return commits
        
    @pytest.fixture
    def sample_deployments(self):
        """Create sample deployments."""
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        deployments = []
        
        # Successful deployment on Jan 3 for commit2 (authored Jan 3)
        deploy1 = Deployment(
            tag_name="v1.0.0",
            name="Release 1.0.0",
            created_at=base_date + timedelta(days=2),  # Jan 3
            published_at=base_date + timedelta(days=2, hours=1),  # Jan 3, 1 hour later
            commit_sha="commit2",
            is_prerelease=False,
        )
        deployments.append(deploy1)
        
        # Failed deployment on Jan 8 for commit4
        deploy2 = Deployment(
            tag_name="v1.1.0",
            name="Release 1.1.0",
            created_at=base_date + timedelta(days=7),  # Jan 8
            published_at=base_date + timedelta(days=7, hours=2),  # Jan 8, 2 hours later
            commit_sha="commit4",
            is_prerelease=False,
        )
        deploy2.deployment_failed = True
        deploy2.failure_resolved_at = base_date + timedelta(days=7, hours=6)
        deployments.append(deploy2)
        
        return deployments
        
    def test_period_boundaries_daily(self, calculator):
        """Test daily period boundary calculation."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 4, tzinfo=timezone.utc)
        
        periods = calculator._get_period_boundaries(start, end, Period.DAILY)
        
        assert len(periods) == 3
        assert periods[0] == (
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc)
        )
        assert periods[2] == (
            datetime(2024, 1, 3, tzinfo=timezone.utc),
            datetime(2024, 1, 4, tzinfo=timezone.utc)
        )
        
    def test_period_boundaries_weekly(self, calculator):
        """Test weekly period boundary calculation."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Monday
        end = datetime(2024, 1, 15, tzinfo=timezone.utc)
        
        periods = calculator._get_period_boundaries(start, end, Period.WEEKLY)
        
        assert len(periods) == 2
        # First week: Jan 1-7 (Mon-Sun)
        assert periods[0][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert periods[0][1] == datetime(2024, 1, 8, tzinfo=timezone.utc)
        # Second week: Jan 8-14 (Mon-Sun)
        assert periods[1][0] == datetime(2024, 1, 8, tzinfo=timezone.utc)
        assert periods[1][1] == datetime(2024, 1, 15, tzinfo=timezone.utc)
        
    def test_period_boundaries_monthly(self, calculator):
        """Test monthly period boundary calculation."""
        start = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end = datetime(2024, 3, 10, tzinfo=timezone.utc)
        
        periods = calculator._get_period_boundaries(start, end, Period.MONTHLY)
        
        assert len(periods) == 3
        # Partial January
        assert periods[0][0] == datetime(2024, 1, 15, tzinfo=timezone.utc)
        assert periods[0][1] == datetime(2024, 2, 1, tzinfo=timezone.utc)
        # Full February
        assert periods[1][0] == datetime(2024, 2, 1, tzinfo=timezone.utc)
        assert periods[1][1] == datetime(2024, 3, 1, tzinfo=timezone.utc)
        # Partial March
        assert periods[2][0] == datetime(2024, 3, 1, tzinfo=timezone.utc)
        assert periods[2][1] == datetime(2024, 3, 10, tzinfo=timezone.utc)
        
    def test_period_boundaries_quarterly(self, calculator):
        """Test quarterly period boundary calculation."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 8, 1, tzinfo=timezone.utc)
        
        periods = calculator._get_period_boundaries(start, end, Period.QUARTERLY)
        
        assert len(periods) == 3
        # Q1
        assert periods[0][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert periods[0][1] == datetime(2024, 4, 1, tzinfo=timezone.utc)
        # Q2
        assert periods[1][0] == datetime(2024, 4, 1, tzinfo=timezone.utc)
        assert periods[1][1] == datetime(2024, 7, 1, tzinfo=timezone.utc)
        # Q3 partial
        assert periods[2][0] == datetime(2024, 7, 1, tzinfo=timezone.utc)
        assert periods[2][1] == datetime(2024, 8, 1, tzinfo=timezone.utc)
        
    def test_period_boundaries_yearly(self, calculator):
        """Test yearly period boundary calculation."""
        start = datetime(2023, 6, 1, tzinfo=timezone.utc)
        end = datetime(2025, 3, 1, tzinfo=timezone.utc)
        
        periods = calculator._get_period_boundaries(start, end, Period.YEARLY)
        
        assert len(periods) == 3
        # Partial 2023
        assert periods[0][0] == datetime(2023, 6, 1, tzinfo=timezone.utc)
        assert periods[0][1] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Full 2024
        assert periods[1][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert periods[1][1] == datetime(2025, 1, 1, tzinfo=timezone.utc)
        # Partial 2025
        assert periods[2][0] == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert periods[2][1] == datetime(2025, 3, 1, tzinfo=timezone.utc)
        
    def test_lead_time_calculation(self, calculator, sample_commits, sample_deployments):
        """Test lead time calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        # Deployment on Jan 3 for commit2 (authored Jan 3)
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 3, tzinfo=timezone.utc),
            datetime(2024, 1, 4, tzinfo=timezone.utc)
        )
        
        lead_time, data_points = calculator._calculate_lead_time(
            deployments,
            datetime(2024, 1, 3, tzinfo=timezone.utc),
            datetime(2024, 1, 4, tzinfo=timezone.utc)
        )
        
        # Lead time should be 1 hour (commit2 authored at Jan 3 00:00, deployed at Jan 3 01:00)
        assert lead_time == 1.0
        assert data_points == 1
        
    def test_deployment_frequency(self, calculator, sample_commits, sample_deployments):
        """Test deployment frequency calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        # Two deployments over 9 days (Jan 1-9)
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        freq, count = calculator._calculate_deployment_frequency(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        # 1 successful deployment (v1.1.0 failed) over 8 days = 0.125 per day
        assert freq == 0.125
        assert count == 1
        
    def test_change_failure_rate(self, calculator, sample_commits, sample_deployments):
        """Test change failure rate calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        rate, failed = calculator._calculate_change_failure_rate(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        # 1 failure out of 2 deployments = 50%
        assert rate == 50.0
        assert failed == 1
        
    def test_mttr_calculation(self, calculator, sample_commits, sample_deployments):
        """Test MTTR calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        mttr, restorations = calculator._calculate_mttr(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        # Failure at hour 2, resolved at hour 6 = 4 hours
        assert mttr == 4.0
        assert restorations == 1
        
    def test_no_deployments(self, calculator):
        """Test metrics when there are no deployments."""
        deployments = []
        
        lead_time, _ = calculator._calculate_lead_time(deployments, None, None)
        assert lead_time is None
        
        freq, count = calculator._calculate_deployment_frequency(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc)
        )
        assert freq == 0.0
        assert count == 0
        
        rate, _ = calculator._calculate_change_failure_rate(deployments, None, None)
        assert rate is None
        
        mttr, _ = calculator._calculate_mttr(deployments, None, None)
        assert mttr is None
        
    def test_manual_deployments(self, calculator, sample_manual_deployments):
        """Test metrics with manual deployments."""
        calculator._build_lookups(sample_manual_deployments, [], [])
        
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 15, tzinfo=timezone.utc)
        )
        
        assert len(deployments) == 2
        
        # Check failure rate
        rate, failed = calculator._calculate_change_failure_rate(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 15, tzinfo=timezone.utc)
        )
        
        # 1 failure out of 2 = 50%
        assert rate == 50.0
        assert failed == 1
        
    def test_rolling_window(self, calculator, sample_commits, sample_deployments):
        """Test rolling window calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        config = MetricsConfig(
            lead_time=MetricConfig(Period.ROLLING_7_DAYS, CalculationMethod.ROLLING_WINDOW),
            reporting_period=Period.DAILY,
        )
        
        # Calculate for a single day with 7-day rolling window
        metrics = calculator._calculate_period_metrics(
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            datetime(2024, 1, 11, tzinfo=timezone.utc),
            config
        )
        
        # Should include deployments from Jan 4-10 (7 days back from Jan 11)
        assert metrics.lead_time_data_points > 0
        
    def test_full_calculation(self, calculator, sample_commits, sample_deployments):
        """Test full metrics calculation."""
        config = MetricsConfig.daily_all()
        
        results = calculator.calculate(
            sample_commits,
            [],
            sample_deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 5, tzinfo=timezone.utc),
            config
        )
        
        assert len(results) == 4  # 4 days
        
        # Check first day has no metrics
        assert results[0].deployment_count == 0
        
        # Check day with deployment
        day_with_deployment = results[2]  # Jan 3
        assert day_with_deployment.deployment_count == 1
        assert day_with_deployment.lead_time_for_changes is not None
        
    def test_metrics_serialization(self):
        """Test DORAMetrics serialization."""
        metrics = DORAMetrics(
            lead_time_for_changes=24.5,
            deployment_frequency=2.0,
            change_failure_rate=10.0,
            mean_time_to_restore=4.0,
            period_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            period_end=datetime(2024, 1, 2, tzinfo=timezone.utc),
            lead_time_data_points=10,
            deployment_count=5,
            failed_deployment_count=1,
            mttr_data_points=1,
        )
        
        # Test to_dict
        data = metrics.to_dict()
        assert data["metrics"]["lead_time_for_changes_hours"] == 24.5
        assert data["metrics"]["deployment_frequency_per_day"] == 2.0
        assert data["context"]["deployment_count"] == 5
        
        # Test to_json
        json_str = metrics.to_json()
        assert "lead_time_for_changes_hours" in json_str
        assert "24.5" in json_str