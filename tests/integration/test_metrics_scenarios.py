"""Integration tests for DORA metrics calculation scenarios."""

from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.calculators.metrics import MetricsCalculator, MetricsConfig, Period
from dora_metrics.models import Commit, Deployment, PRState, PullRequest


@pytest.mark.integration
class TestMetricsScenarios:
    """Test realistic DORA metrics scenarios."""
    
    def test_high_performing_team(self):
        """Test metrics for a high-performing team."""
        # High performers: multiple daily deployments, low failure rate, quick recovery
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        deployments = []
        
        # Create 2 deployments per day for a week
        for day in range(7):
            for deploy_num in range(2):
                commit_time = base_date + timedelta(days=day, hours=deploy_num * 8)
                deploy_time = commit_time + timedelta(hours=2)  # 2-hour lead time
                
                commit = Commit(
                    sha=f"commit_{day}_{deploy_num}",
                    author_name="Dev",
                    author_email="dev@example.com",
                    authored_date=commit_time,
                    committer_name="Dev",
                    committer_email="dev@example.com",
                    committed_date=commit_time,
                    message=f"Feature {day}-{deploy_num}",
                    files_changed=["app.py"],
                    additions=20,
                    deletions=5,
                )
                commits.append(commit)
                
                deployment = Deployment(
                    tag_name=f"v1.{day}.{deploy_num}",
                    name=f"Deploy {day}-{deploy_num}",
                    created_at=deploy_time,
                    published_at=deploy_time,
                    commit_sha=commit.sha,
                    is_prerelease=False,
                )
                
                # Only 1 failure in the week
                if day == 3 and deploy_num == 1:
                    deployment.deployment_failed = True
                    deployment.failure_resolved_at = deploy_time + timedelta(minutes=30)
                    
                deployments.append(deployment)
                
        # Calculate weekly metrics
        config = MetricsConfig(reporting_period=Period.WEEKLY)
        results = calculator.calculate(
            commits, [], deployments,
            base_date,
            base_date + timedelta(days=7),
            config
        )
        
        assert len(results) == 1  # One week
        metrics = results[0]
        
        # High deployment frequency (13 successful out of 14 total = 1.86 per day)
        assert metrics.deployment_frequency > 1.8
        assert metrics.deployment_frequency < 2.0
        
        # Low lead time (2 hours)
        assert metrics.lead_time_for_changes == 2.0
        
        # Low failure rate (1/14 ≈ 7%)
        assert 7.0 <= metrics.change_failure_rate <= 8.0
        
        # Fast MTTR (30 minutes = 0.5 hours)
        assert metrics.mean_time_to_restore == 0.5
        
    def test_low_performing_team(self):
        """Test metrics for a low-performing team."""
        # Low performers: weekly deployments, high failure rate, slow recovery
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        deployments = []
        
        # Create 1 deployment per week for a month, with long lead times
        for week in range(4):
            commit_time = base_date + timedelta(weeks=week)
            deploy_time = commit_time + timedelta(days=5)  # 5-day lead time
            
            commit = Commit(
                sha=f"commit_week_{week}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=commit_time,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=commit_time,
                message=f"Weekly release {week}",
                files_changed=["app.py"],
                additions=100,
                deletions=50,
            )
            commits.append(commit)
            
            deployment = Deployment(
                tag_name=f"v1.{week}.0",
                name=f"Weekly Release {week}",
                created_at=deploy_time,
                published_at=deploy_time,
                commit_sha=commit.sha,
                is_prerelease=False,
            )
            
            # 50% failure rate
            if week % 2 == 1:
                deployment.deployment_failed = True
                deployment.failure_resolved_at = deploy_time + timedelta(days=2)  # 2-day MTTR
                
            deployments.append(deployment)
            
        # Calculate monthly metrics
        config = MetricsConfig(reporting_period=Period.MONTHLY)
        results = calculator.calculate(
            commits, [], deployments,
            base_date,
            base_date + timedelta(days=30),
            config
        )
        
        assert len(results) == 1  # One month
        metrics = results[0]
        
        # Low deployment frequency (2 successful out of 4 in 30 days)
        assert metrics.deployment_frequency < 0.1
        
        # High lead time (5 days = 120 hours)
        assert metrics.lead_time_for_changes == 120.0
        
        # High failure rate (50%)
        assert metrics.change_failure_rate == 50.0
        
        # Slow MTTR (2 days = 48 hours)
        assert metrics.mean_time_to_restore == 48.0
        
    def test_improving_team_trend(self):
        """Test metrics showing team improvement over time."""
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        deployments = []
        
        # Create improving metrics over 3 months
        for month in range(3):
            month_start = base_date + timedelta(days=month * 30)
            
            # Increasing deployment frequency each month
            deploys_per_week = month + 1
            
            for week in range(4):
                for deploy in range(deploys_per_week):
                    commit_time = month_start + timedelta(weeks=week, days=deploy)
                    # Decreasing lead time each month
                    lead_time_hours = 48 / (month + 1)
                    deploy_time = commit_time + timedelta(hours=lead_time_hours)
                    
                    commit = Commit(
                        sha=f"commit_m{month}_w{week}_d{deploy}",
                        author_name="Dev",
                        author_email="dev@example.com",
                        authored_date=commit_time,
                        committer_name="Dev",
                        committer_email="dev@example.com",
                        committed_date=commit_time,
                        message=f"Deploy {month}-{week}-{deploy}",
                        files_changed=["app.py"],
                        additions=30,
                        deletions=10,
                    )
                    commits.append(commit)
                    
                    deployment = Deployment(
                        tag_name=f"v{month}.{week}.{deploy}",
                        name=f"Deploy {month}-{week}-{deploy}",
                        created_at=deploy_time,
                        published_at=deploy_time,
                        commit_sha=commit.sha,
                        is_prerelease=False,
                    )
                    
                    # Decreasing failure rate each month
                    failure_rate = 0.3 - (month * 0.1)  # 30%, 20%, 10%
                    if (week * deploys_per_week + deploy) % int(1/failure_rate) == 0:
                        deployment.deployment_failed = True
                        # Improving MTTR each month
                        mttr_hours = 24 / (month + 1)
                        deployment.failure_resolved_at = deploy_time + timedelta(hours=mttr_hours)
                        
                    deployments.append(deployment)
                    
        # Calculate monthly metrics
        config = MetricsConfig(reporting_period=Period.MONTHLY)
        results = calculator.calculate(
            commits, [], deployments,
            base_date,
            base_date + timedelta(days=90),
            config
        )
        
        assert len(results) == 3  # Three months
        
        # Verify improving trends
        # Deployment frequency increases
        assert results[0].deployment_frequency < results[1].deployment_frequency
        assert results[1].deployment_frequency < results[2].deployment_frequency
        
        # Lead time decreases
        assert results[0].lead_time_for_changes > results[1].lead_time_for_changes
        assert results[1].lead_time_for_changes > results[2].lead_time_for_changes
        
        # Failure rate decreases
        assert results[0].change_failure_rate > results[1].change_failure_rate
        assert results[1].change_failure_rate > results[2].change_failure_rate
        
        # MTTR decreases
        assert results[0].mean_time_to_restore > results[1].mean_time_to_restore
        assert results[1].mean_time_to_restore > results[2].mean_time_to_restore
        
    def test_mixed_deployment_sources(self):
        """Test metrics with both GitHub and manual deployments."""
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        deployments = []
        
        # GitHub deployment
        commit1 = Commit(
            sha="github1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date,
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date,
            message="GitHub deploy",
            files_changed=["app.py"],
            additions=50,
            deletions=20,
        )
        commits.append(commit1)
        
        deployment1 = Deployment(
            tag_name="v1.0.0",
            name="GitHub Release",
            created_at=base_date + timedelta(hours=4),
            published_at=base_date + timedelta(hours=4),
            commit_sha=commit1.sha,
            is_prerelease=False,
        )
        deployments.append(deployment1)
        
        # Manual deployment
        commit2 = Commit(
            sha="manual1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=1),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=1),
            message="Manual deploy",
            files_changed=["config.py"],
            additions=10,
            deletions=5,
        )
        commit2.is_manual_deployment = True
        commit2.manual_deployment_timestamp = base_date + timedelta(days=1, hours=6)
        commit2.manual_deployment_failed = False
        commits.append(commit2)
        
        # Failed manual deployment
        commit3 = Commit(
            sha="manual2",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(days=2),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(days=2),
            message="Failed manual deploy",
            files_changed=["db.py"],
            additions=30,
            deletions=15,
        )
        commit3.is_manual_deployment = True
        commit3.manual_deployment_timestamp = base_date + timedelta(days=2, hours=3)
        commit3.manual_deployment_failed = True
        commits.append(commit3)
        
        # Calculate weekly metrics
        config = MetricsConfig(reporting_period=Period.WEEKLY)
        results = calculator.calculate(
            commits, [], deployments,
            base_date,
            base_date + timedelta(days=7),
            config
        )
        
        metrics = results[0]
        
        # 2 successful deployments (GitHub + manual) in 7 days
        assert metrics.deployment_frequency == 2.0 / 7.0
        assert metrics.deployment_count == 2
        
        # 1 failure out of 3 total
        assert abs(metrics.change_failure_rate - (100.0 / 3.0)) < 0.01
        assert metrics.failed_deployment_count == 1
        
        # Lead times for all deployments
        assert metrics.lead_time_data_points == 3  # All deployments (successful and failed)
        
    def test_rolling_window_metrics(self):
        """Test rolling window calculations provide stability."""
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        commits = []
        deployments = []
        
        # Create sporadic deployments
        deploy_days = [0, 1, 7, 8, 14, 20, 21]  # Clustered deployments
        
        for day in deploy_days:
            commit = Commit(
                sha=f"commit_day_{day}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(days=day),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(days=day),
                message=f"Deploy day {day}",
                files_changed=["app.py"],
                additions=20,
                deletions=10,
            )
            commits.append(commit)
            
            deployment = Deployment(
                tag_name=f"v1.{day}",
                name=f"Deploy {day}",
                created_at=base_date + timedelta(days=day, hours=2),
                published_at=base_date + timedelta(days=day, hours=2),
                commit_sha=commit.sha,
                is_prerelease=False,
            )
            deployments.append(deployment)
            
        # Compare daily vs rolling window
        config_daily = MetricsConfig.daily_all()
        config_rolling = MetricsConfig.recommended()  # Uses rolling windows
        
        results_daily = calculator.calculate(
            commits, [], deployments,
            base_date + timedelta(days=15),
            base_date + timedelta(days=25),
            config_daily
        )
        
        results_rolling = calculator.calculate(
            commits, [], deployments,
            base_date + timedelta(days=15),
            base_date + timedelta(days=25),
            config_rolling
        )
        
        # Daily metrics should be highly variable
        daily_frequencies = [m.deployment_frequency for m in results_daily]
        daily_variance = max(daily_frequencies) - min(daily_frequencies)
        
        # Rolling window metrics should be more stable
        rolling_frequencies = [m.deployment_frequency for m in results_rolling]
        rolling_variance = max(rolling_frequencies) - min(rolling_frequencies)
        
        # Rolling window should have less variance
        assert rolling_variance < daily_variance
        
    def test_no_data_periods(self):
        """Test handling of periods with no deployments."""
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Only deployments in first and last week of month
        commits = []
        deployments = []
        
        for day in [1, 2, 28, 29]:
            commit = Commit(
                sha=f"commit_day_{day}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(days=day),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(days=day),
                message=f"Deploy day {day}",
                files_changed=["app.py"],
                additions=10,
                deletions=5,
            )
            commits.append(commit)
            
            deployment = Deployment(
                tag_name=f"v1.{day}",
                name=f"Deploy {day}",
                created_at=base_date + timedelta(days=day, hours=1),
                published_at=base_date + timedelta(days=day, hours=1),
                commit_sha=commit.sha,
                is_prerelease=False,
            )
            deployments.append(deployment)
            
        # Calculate weekly metrics
        config = MetricsConfig(reporting_period=Period.WEEKLY)
        results = calculator.calculate(
            commits, [], deployments,
            base_date,
            base_date + timedelta(days=30),
            config
        )
        
        # Should have multiple weeks
        assert len(results) > 4
        
        # Middle weeks should have no deployments
        middle_weeks = results[1:3]
        for week_metrics in middle_weeks:
            assert week_metrics.deployment_count == 0
            assert week_metrics.deployment_frequency == 0.0
            assert week_metrics.lead_time_for_changes is None
            assert week_metrics.change_failure_rate is None
            assert week_metrics.mean_time_to_restore is None
    
    def test_hotfix_scenario(self):
        """Test metrics for hotfix deployments with very short lead times."""
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)  # 2pm Monday
        
        commits = []
        deployments = []
        prs = []
        
        # Normal morning deployment
        normal_commit = Commit(
            sha="normal_deploy",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date - timedelta(days=2),  # Worked on Friday
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date - timedelta(days=2),
            message="Feature: Add user dashboard",
            files_changed=["dashboard.py"],
            additions=200,
            deletions=50,
        )
        normal_commit.pr_number = 100
        commits.append(normal_commit)
        
        normal_pr = PullRequest(
            number=100,
            title="Add user dashboard",
            state=PRState.MERGED,
            created_at=base_date - timedelta(days=3),
            updated_at=base_date - timedelta(days=2, hours=8),
            closed_at=base_date - timedelta(days=2, hours=8),
            merged_at=base_date - timedelta(days=2, hours=8),
            merge_commit_sha="normal_deploy",
            author="dev",
            labels=["feature"],
            commits=["normal_deploy"],
        )
        prs.append(normal_pr)
        
        normal_deployment = Deployment(
            tag_name="v2.0.0",
            name="Morning release",
            created_at=base_date - timedelta(hours=6),  # 8am
            published_at=base_date - timedelta(hours=6),
            commit_sha="normal_deploy",
            is_prerelease=False,
        )
        deployments.append(normal_deployment)
        
        # Production issue discovered at 2pm, hotfix cycle begins
        
        # Hotfix 1: Critical bug fix (30 minute turnaround)
        hotfix1 = Commit(
            sha="hotfix1",
            author_name="SRE",
            author_email="sre@example.com",
            authored_date=base_date + timedelta(minutes=10),  # 2:10pm
            committer_name="SRE",
            committer_email="sre@example.com",
            committed_date=base_date + timedelta(minutes=15),  # 2:15pm
            message="HOTFIX: Fix null pointer in payment processing",
            files_changed=["payment.py"],
            additions=5,
            deletions=2,
        )
        hotfix1.pr_number = 101
        commits.append(hotfix1)
        
        hotfix1_pr = PullRequest(
            number=101,
            title="URGENT: Fix payment processing NPE",
            state=PRState.MERGED,
            created_at=base_date + timedelta(minutes=15),
            updated_at=base_date + timedelta(minutes=25),
            closed_at=base_date + timedelta(minutes=25),
            merged_at=base_date + timedelta(minutes=25),  # 10 min review
            merge_commit_sha="hotfix1",
            author="sre",
            labels=["hotfix", "critical", "production-issue"],
            commits=["hotfix1"],
        )
        prs.append(hotfix1_pr)
        
        hotfix1_deployment = Deployment(
            tag_name="v2.0.1",
            name="Hotfix - Payment NPE",
            created_at=base_date + timedelta(minutes=30),  # 2:30pm
            published_at=base_date + timedelta(minutes=30),
            commit_sha="hotfix1",
            is_prerelease=False,
        )
        deployments.append(hotfix1_deployment)
        
        # Hotfix 2: Follow-up fix needed (45 minute turnaround)
        hotfix2 = Commit(
            sha="hotfix2",
            author_name="SRE",
            author_email="sre@example.com",
            authored_date=base_date + timedelta(minutes=40),  # 2:40pm
            committer_name="SRE",
            committer_email="sre@example.com",
            committed_date=base_date + timedelta(minutes=45),  # 2:45pm
            message="HOTFIX: Add logging for payment failures",
            files_changed=["payment.py", "logging.py"],
            additions=15,
            deletions=3,
        )
        hotfix2.pr_number = 102
        commits.append(hotfix2)
        
        hotfix2_pr = PullRequest(
            number=102,
            title="URGENT: Add payment failure logging",
            state=PRState.MERGED,
            created_at=base_date + timedelta(minutes=45),
            updated_at=base_date + timedelta(minutes=60),
            closed_at=base_date + timedelta(minutes=60),
            merged_at=base_date + timedelta(minutes=60),  # 15 min review
            merge_commit_sha="hotfix2",
            author="sre",
            labels=["hotfix", "urgent"],
            commits=["hotfix2"],
        )
        prs.append(hotfix2_pr)
        
        hotfix2_deployment = Deployment(
            tag_name="v2.0.2",
            name="Hotfix - Payment logging",
            created_at=base_date + timedelta(minutes=75),  # 3:15pm
            published_at=base_date + timedelta(minutes=75),
            commit_sha="hotfix2",
            is_prerelease=False,
        )
        deployments.append(hotfix2_deployment)
        
        # Calculate daily metrics
        config = MetricsConfig.daily_all()
        results = calculator.calculate(
            commits, prs, deployments,
            base_date - timedelta(days=3),
            base_date + timedelta(days=1),
            config
        )
        
        # Find the hotfix day
        hotfix_day = None
        for result in results:
            if result.deployment_count == 2:  # Day with both hotfixes
                hotfix_day = result
                break
        
        assert hotfix_day is not None, "Should find day with hotfixes"
        
        # Verify hotfix metrics characteristics
        assert hotfix_day.deployment_count == 2
        assert hotfix_day.deployment_frequency == 2.0  # 2 deployments in one day
        
        # Lead time should be very short (median of 0.33 and 0.58 hours)
        assert hotfix_day.lead_time_for_changes < 1.0, \
            f"Hotfixes should have <1 hour lead time, got {hotfix_day.lead_time_for_changes}"
        
        # Should have data from both hotfixes
        assert hotfix_day.lead_time_data_points == 2
        
        # Neither hotfix failed
        assert hotfix_day.change_failure_rate == 0.0
        
        # Compare with normal deployment day
        normal_day = None
        for result in results:
            if result.deployment_count == 1 and result.lead_time_for_changes and result.lead_time_for_changes > 24:
                normal_day = result
                break
                
        assert normal_day is not None, "Should find normal deployment day"
        
        # Normal deployment should have much longer lead time
        assert normal_day.lead_time_for_changes > 24.0, \
            "Normal deployment should have >24 hour lead time"
        
        # This demonstrates that DORA metrics work as a system:
        # - Deployment frequency is high (2/day) 
        # - Lead time looks great (<1 hour)
        # - Change failure rate tells the real story
        #
        # If these hotfixes were due to earlier failures, the failure rate
        # would reveal the unhealthy pattern. The metrics are self-balancing:
        # you can't game frequency without failure rate exposing the problem.
    
    def test_hotfix_cascade_scenario(self):
        """
        Test metrics for a cascade of failures and hotfixes.
        This shows how failure rate reveals unhealthy high frequency.
        """
        calculator = MetricsCalculator()
        base_date = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)  # 10am
        
        commits = []
        deployments = []
        
        # Initial deployment that will fail
        commit1 = Commit(
            sha="feature1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date - timedelta(hours=24),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date - timedelta(hours=24),
            message="Feature: New payment provider",
            files_changed=["payment.py"],
            additions=200,
            deletions=50,
        )
        commits.append(commit1)
        
        deployment1 = Deployment(
            tag_name="v1.0.0",
            name="Feature release",
            created_at=base_date,
            published_at=base_date,
            commit_sha="feature1",
            is_prerelease=False,
        )
        deployment1.deployment_failed = True
        deployment1.failure_resolved_at = base_date + timedelta(hours=4)
        deployments.append(deployment1)
        
        # Hotfix 1 - also fails
        commit2 = Commit(
            sha="hotfix1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(minutes=30),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(minutes=30),
            message="HOTFIX: Fix payment NPE",
            files_changed=["payment.py"],
            additions=10,
            deletions=5,
        )
        commits.append(commit2)
        
        deployment2 = Deployment(
            tag_name="v1.0.1",
            name="Hotfix 1",
            created_at=base_date + timedelta(hours=1),
            published_at=base_date + timedelta(hours=1),
            commit_sha="hotfix1",
            is_prerelease=False,
        )
        deployment2.deployment_failed = True
        deployment2.failure_resolved_at = base_date + timedelta(hours=3)
        deployments.append(deployment2)
        
        # Hotfix 2 - finally works
        commit3 = Commit(
            sha="hotfix2",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date + timedelta(hours=2),
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date + timedelta(hours=2),
            message="HOTFIX: Properly fix payment issue",
            files_changed=["payment.py"],
            additions=15,
            deletions=8,
        )
        commits.append(commit3)
        
        deployment3 = Deployment(
            tag_name="v1.0.2",
            name="Hotfix 2",
            created_at=base_date + timedelta(hours=3),
            published_at=base_date + timedelta(hours=3),
            commit_sha="hotfix2",
            is_prerelease=False,
        )
        deployments.append(deployment3)
        
        # Calculate metrics incrementally to show how the day unfolds
        from dora_metrics.calculators.metrics import MetricsConfig
        config = MetricsConfig.daily_all()
        
        # Metric snapshot 1: After initial deployment (10am)
        results_10am = calculator.calculate(
            commits[:1], [], deployments[:1],
            base_date - timedelta(days=1),
            base_date + timedelta(hours=1),
            config
        )
        metrics_10am = results_10am[1]
        
        # At 10am: One failed deployment
        assert metrics_10am.deployment_frequency == 0.0  # No successful deployments yet
        assert metrics_10am.deployment_count == 0
        assert metrics_10am.change_failure_rate == 100.0  # 1 out of 1 failed
        assert metrics_10am.failed_deployment_count == 1
        
        # Metric snapshot 2: After first hotfix attempt (11am)
        results_11am = calculator.calculate(
            commits[:2], [], deployments[:2],
            base_date - timedelta(days=1),
            base_date + timedelta(hours=2),
            config
        )
        metrics_11am = results_11am[1]
        
        # At 11am: Two failed deployments, panic setting in
        assert metrics_11am.deployment_frequency == 0.0  # Still no successful deployments
        assert metrics_11am.deployment_count == 0
        assert metrics_11am.change_failure_rate == 100.0  # 2 out of 2 failed!
        assert metrics_11am.failed_deployment_count == 2
        
        # Metric snapshot 3: After successful hotfix (1pm)
        results_1pm = calculator.calculate(
            commits, [], deployments,
            base_date - timedelta(days=1),
            base_date + timedelta(days=1),
            config
        )
        crisis_day = results_1pm[1]  # Final state of the day
        
        # Verify which deployments are counted
        all_deployments_in_period = calculator._get_deployments_in_period(
            crisis_day.period_start,
            crisis_day.period_end
        )
        
        # Should have 3 total deployments
        assert len(all_deployments_in_period) == 3
        
        # Verify the successful deployment is the final hotfix (v1.0.2)
        successful_deployments = [
            (d[1], d[2]) for d in all_deployments_in_period 
            if not calculator._is_deployment_failed(d[2] if d[2] else d[1])
        ]
        assert len(successful_deployments) == 1
        assert successful_deployments[0][0].sha == "hotfix2"  # The final fix that worked
        
        # Deployment frequency only counts successful deployments (DORA standard)
        assert crisis_day.deployment_frequency == 1.0  # Only 1 successful deployment
        assert crisis_day.deployment_count == 1  # Successful deployments
        
        # Low lead time looks "good" 
        assert crisis_day.lead_time_for_changes < 4.0  # Fast deployments!
        
        # But failure rate tells the real story
        assert crisis_day.change_failure_rate > 60.0  # 2 out of 3 failed!
        assert crisis_day.failed_deployment_count == 2
        
        # And MTTR shows the team spent hours fixing issues
        assert crisis_day.mean_time_to_restore > 1.0  # Hours spent firefighting
        
        # This pattern reveals the truth: even though frequency appears low,
        # the high failure rate (66%) and significant MTTR show the team
        # spent the day firefighting, not delivering value.
        
        # The incremental view shows the story:
        # - 10am: Feature fails, 100% failure rate, frequency = 0
        # - 11am: First hotfix fails too, still 100% failure rate, frequency = 0  
        # - 1pm: Finally fixed, but 66% failure rate reveals the chaos
        # This demonstrates why viewing metrics together matters - frequency alone
        # would show "1 deployment today" but miss the 3 hours of crisis.