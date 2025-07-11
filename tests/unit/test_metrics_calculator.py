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
        
        # 1 failure out of 2 deployments = 0.5 (50% as ratio)
        assert rate == 0.5
        assert failed == 1
        
    def test_mttr_calculation(self, calculator, sample_commits, sample_deployments):
        """Test MTTR calculation."""
        calculator._build_lookups(sample_commits, [], sample_deployments)
        
        deployments = calculator._get_deployments_in_period(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        mttr, restorations, mttr_stats = calculator._calculate_mttr(
            deployments,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 9, tzinfo=timezone.utc)
        )
        
        # Failure at hour 2, resolved at hour 6 = 4 hours
        assert mttr == 4.0
        assert restorations == 1
        assert mttr_stats['p50'] == 4.0  # Only one data point
        
    def test_no_deployments(self, calculator):
        """Test metrics when there are no deployments."""
        deployments = []
        
        lead_time, _, _ = calculator._calculate_lead_time(deployments, None, None)
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
        
        mttr, _, _ = calculator._calculate_mttr(deployments, None, None)
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
        
        # 1 failure out of 2 = 0.5 (50% as ratio)
        assert rate == 0.5
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
            change_failure_rate=0.1,
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
        
    def test_lead_time_should_include_all_commits_between_deployments(self, calculator):
        """
        Test that lead time calculation includes ALL commits deployed,
        not just the deployment commit itself.
        
        Current behavior: Only deployment commit's lead time is calculated
        Expected behavior: All commits since last deployment should be included
        """
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        
        # First deployment with single commit
        commit1 = Commit(
            sha="commit1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date,
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date,
            message="Initial commit",
            files_changed=["file1.py"],
            additions=100,
            deletions=0,
        )
        commits.append(commit1)
        
        deployment1 = Deployment(
            tag_name="v1.0.0",
            name="First release",
            created_at=base_date + timedelta(hours=1),
            published_at=base_date + timedelta(hours=1),
            commit_sha="commit1",
            is_prerelease=False,
        )
        
        # Multiple commits for second deployment
        # These represent work done between v1.0.0 and v1.1.0
        commit2 = Commit(
            sha="commit2",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=1),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=1),
            message="Feature A",
            files_changed=["feature_a.py"],
            additions=50,
            deletions=10,
        )
        commits.append(commit2)
        
        commit3 = Commit(
            sha="commit3",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=2),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=2),
            message="Feature B",
            files_changed=["feature_b.py"],
            additions=75,
            deletions=5,
        )
        commits.append(commit3)
        
        # Deployment commit
        commit4 = Commit(
            sha="commit4",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=3),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=3),
            message="Release prep v1.1.0",
            files_changed=["version.py"],
            additions=2,
            deletions=2,
        )
        commits.append(commit4)
        
        deployment2 = Deployment(
            tag_name="v1.1.0",
            name="Second release",
            created_at=base_date + timedelta(days=3, hours=2),
            published_at=base_date + timedelta(days=3, hours=2),
            commit_sha="commit4",
            is_prerelease=False,
        )
        
        # Calculate metrics with daily reporting
        from dora_metrics.calculators.metrics import MetricsConfig
        config = MetricsConfig.daily_all()
        
        results = calculator.calculate(
            commits,
            [],
            [deployment1, deployment2],
            base_date,
            base_date + timedelta(days=4),
            config
        )
        
        # Debug: check results length
        assert len(results) == 4, f"Expected 4 daily results, got {len(results)}"
        
        # Get the lead time for the second deployment period
        day3_metrics = results[3]  # Day 3 when deployment2 happened
        
        # CURRENT BEHAVIOR: Only tracks commit4's lead time (2 hours)
        # EXPECTED BEHAVIOR: Should track lead times for commit2, commit3, and commit4
        # - commit2: (day 3 + 2h) - (day 1) = 50 hours
        # - commit3: (day 3 + 2h) - (day 2) = 26 hours  
        # - commit4: (day 3 + 2h) - (day 3) = 2 hours
        # Median of [2, 26, 50] = 26 hours
        
        # This test will FAIL with current implementation
        assert day3_metrics.lead_time_for_changes == 26.0, \
            f"Expected median lead time of 26h for all commits, got {day3_metrics.lead_time_for_changes}h"
        
        # Also check that we counted all commits
        assert day3_metrics.lead_time_data_points == 3, \
            f"Expected 3 data points (all commits since v1.0.0), got {day3_metrics.lead_time_data_points}"
    
    def test_lead_time_percentiles(self, calculator):
        """
        Test that lead time calculation includes percentile statistics.
        """
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Create commits with varying ages
        commits = []
        
        # Create commits at different times to create varying lead times
        # When deployed together, they'll have lead times of: 1, 2, 3, 4, 5, 10, 20, 30, 40, 100 hours
        hours_before_deploy = [1, 2, 3, 4, 5, 10, 20, 30, 40, 100]
        deploy_time = base_date + timedelta(days=5)  # Deploy on day 5
        
        for i, hours in enumerate(hours_before_deploy):
            commit = Commit(
                sha=f"commit_{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=deploy_time - timedelta(hours=hours),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=deploy_time - timedelta(hours=hours),
                message=f"Commit {i} - {hours}h before deploy",
                files_changed=[f"file{i}.py"],
                additions=10,
                deletions=5,
            )
            commits.append(commit)
        
        # Single deployment that includes all commits
        deployment = Deployment(
            tag_name="v2.0.0",
            name="Big Release with Statistics",
            created_at=deploy_time,
            published_at=deploy_time,
            commit_sha="commit_9",  # The last commit (oldest, 100h lead time)
            is_prerelease=False,
        )
        
        # Calculate metrics
        config = MetricsConfig.daily_all()
        results = calculator.calculate(
            commits,
            [],
            [deployment],
            base_date,
            base_date + timedelta(days=6),
            config
        )
        
        # Get the deployment day metrics
        day_metrics = results[5]  # Day 5 when deployment happened
        
        # Verify we have all the commits
        assert day_metrics.lead_time_data_points == 10
        
        # Check the median (p50)
        assert day_metrics.lead_time_for_changes == 7.5  # Median of [1,2,3,4,5,10,20,30,40,100]
        assert day_metrics.lead_time_p50 == 7.5
        
        # Check percentiles (using approx for floating point)
        assert abs(day_metrics.lead_time_p90 - 46.0) < 0.1  # 90th percentile
        assert abs(day_metrics.lead_time_p95 - 73.0) < 0.1  # 95th percentile
        
        # Check mean is affected by the outlier
        assert day_metrics.lead_time_mean == 21.5  # Mean of all values
        
        # Check standard deviation is high due to outlier
        assert day_metrics.lead_time_std_dev > 28  # High variation
        
        # Check min/max
        assert day_metrics.lead_time_min == 1.0
        assert day_metrics.lead_time_max == 100.0
    
    def test_manual_deployment_failed_as_boolean(self, calculator):
        """Test that manual_deployment_failed works with boolean values (as CSV import produces)."""
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Create commits with manual deployments
        # Note: CSV import converts string "true"/"false" to boolean True/False
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Deploy v1.0",
                is_manual_deployment=True,
                manual_deployment_timestamp=base_date,
                manual_deployment_failed=False,  # Boolean False (as CSV import produces)
                deployment_tag="v1.0"
            ),
            Commit(
                sha="commit2",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date.replace(day=2),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date.replace(day=2),
                message="Deploy v1.1 - hotfix",
                is_manual_deployment=True,
                manual_deployment_timestamp=base_date.replace(day=2),
                manual_deployment_failed=True,  # Boolean True (as CSV import produces)
                deployment_tag="v1.1"
            ),
            Commit(
                sha="commit3",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date.replace(day=3),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date.replace(day=3),
                message="Deploy v1.2",
                is_manual_deployment=True,
                manual_deployment_timestamp=base_date.replace(day=3),
                manual_deployment_failed=False,  # Boolean False (as CSV import produces)
                deployment_tag="v1.2"
            ),
        ]
        
        # Calculate metrics
        config = MetricsConfig(reporting_period=Period.MONTHLY)
        metrics_list = calculator.calculate(
            commits=commits,
            pull_requests=[],
            deployments=[],
            start_date=base_date,
            end_date=base_date.replace(month=2),
            config=config
        )
        
        assert len(metrics_list) == 1
        metrics = metrics_list[0]
        
        # Should have 2 successful deployments, 1 failed
        # deployment_count only counts successful deployments (DORA standard)
        assert metrics.deployment_count == 2
        assert metrics.failed_deployment_count == 1
        # Change failure rate should be 0.3333 (1/3 total deployments)
        assert metrics.change_failure_rate == pytest.approx(0.3333, rel=0.01)