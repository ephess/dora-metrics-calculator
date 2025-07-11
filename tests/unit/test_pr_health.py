"""Unit tests for PR health analyzer."""

from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.analyzers.pr_health import (
    PRHealthAnalyzer,
    PRHealthMetrics,
    PRHealthReport,
    PRHealthStatus,
    PRSize,
)
from dora_metrics.models import PRState, PullRequest


@pytest.mark.unit
class TestPRHealthAnalyzer:
    """Test PR health analysis functionality."""
    
    @pytest.fixture
    def analyzer(self):
        """Create analyzer with fixed reference time."""
        reference_time = datetime(2024, 1, 15, tzinfo=timezone.utc)
        return PRHealthAnalyzer(reference_time=reference_time)
    
    @pytest.fixture
    def base_date(self):
        """Base date for creating test data."""
        return datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    def test_empty_pr_list(self, analyzer):
        """Test analysis with no PRs."""
        report = analyzer.analyze([])
        
        assert report.total_open_prs == 0
        assert report.active_count == 0
        assert report.stale_count == 0
        assert report.abandoned_count == 0
        assert len(report.recommendations) == 0
    
    def test_all_closed_prs(self, analyzer, base_date):
        """Test analysis with only closed PRs."""
        prs = [
            PullRequest(
                number=1,
                title="Closed PR",
                state=PRState.CLOSED,
                created_at=base_date,
                updated_at=base_date + timedelta(days=1),
                closed_at=base_date + timedelta(days=1),
                merged_at=base_date + timedelta(days=1),
                merge_commit_sha="abc123",
                author="dev",
                labels=[],
                commits=["commit1"],
                additions=50,
                deletions=10,
            )
        ]
        
        report = analyzer.analyze(prs)
        assert report.total_open_prs == 0
    
    def test_active_pr_categorization(self, analyzer, base_date):
        """Test PRs are correctly categorized as active."""
        # PR with activity 3 days ago (active)
        pr = PullRequest(
            number=1,
            title="Active PR",
            state=PRState.OPEN,
            created_at=base_date,
            updated_at=base_date + timedelta(days=12),  # 3 days before reference time
            closed_at=None,
            merged_at=None,
            merge_commit_sha=None,
            author="dev",
            labels=[],
            commits=["commit1"],
            additions=50,
            deletions=10,
        )
        
        report = analyzer.analyze([pr])
        
        assert report.total_open_prs == 1
        assert report.active_count == 1
        assert report.stale_count == 0
        assert report.abandoned_count == 0
        assert len(report.active_prs) == 1
        assert report.active_prs[0].status == PRHealthStatus.ACTIVE
        assert report.active_prs[0].days_since_activity == 2  # Jan 13 to Jan 15 = 2 days
    
    def test_stale_pr_categorization(self, analyzer, base_date):
        """Test PRs are correctly categorized as stale."""
        # PR with activity 10 days ago (stale)
        pr = PullRequest(
            number=2,
            title="Stale PR",
            state=PRState.OPEN,
            created_at=base_date,
            updated_at=base_date + timedelta(days=5),  # 10 days before reference time
            closed_at=None,
            merged_at=None,
            merge_commit_sha=None,
            author="dev",
            labels=[],
            commits=["commit1"],
            additions=150,
            deletions=50,
        )
        
        report = analyzer.analyze([pr])
        
        assert report.total_open_prs == 1
        assert report.active_count == 0
        assert report.stale_count == 1
        assert report.abandoned_count == 0
        assert len(report.stale_prs) == 1
        assert report.stale_prs[0].status == PRHealthStatus.STALE
        assert report.stale_prs[0].days_since_activity == 9  # Jan 6 to Jan 15 = 9 days
        assert report.total_stale_days == 9
    
    def test_abandoned_pr_categorization(self, analyzer, base_date):
        """Test PRs are correctly categorized as abandoned."""
        # PR with activity 45 days ago (abandoned)
        pr = PullRequest(
            number=3,
            title="Abandoned PR",
            state=PRState.OPEN,
            created_at=base_date - timedelta(days=30),  # Created 45 days ago
            updated_at=base_date - timedelta(days=30),  # No updates since creation
            closed_at=None,
            merged_at=None,
            merge_commit_sha=None,
            author="dev",
            labels=[],
            commits=["commit1"],
            additions=1000,
            deletions=500,
        )
        
        report = analyzer.analyze([pr])
        
        assert report.total_open_prs == 1
        assert report.active_count == 0
        assert report.stale_count == 0
        assert report.abandoned_count == 1
        assert len(report.abandoned_prs) == 1
        assert report.abandoned_prs[0].status == PRHealthStatus.ABANDONED
        assert report.abandoned_prs[0].days_since_activity == 44  # Dec 2 to Jan 15 = 44 days
        assert report.total_abandoned_days == 44
    
    def test_pr_size_categorization(self, analyzer, base_date):
        """Test PR size categorization."""
        prs = [
            # Small PR (<100 lines)
            PullRequest(
                number=1,
                title="Small PR",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=10),
                updated_at=base_date + timedelta(days=14),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["commit1"],
                additions=30,
                deletions=20,  # Total: 50 lines
            ),
            # Medium PR (100-500 lines)
            PullRequest(
                number=2,
                title="Medium PR",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=10),
                updated_at=base_date + timedelta(days=14),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["commit1", "commit2"],
                additions=200,
                deletions=100,  # Total: 300 lines
            ),
            # Large PR (>500 lines)
            PullRequest(
                number=3,
                title="Large PR",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=10),
                updated_at=base_date + timedelta(days=14),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["commit1", "commit2", "commit3"],
                additions=800,
                deletions=300,  # Total: 1100 lines
            ),
        ]
        
        report = analyzer.analyze(prs)
        
        assert report.small_count == 1
        assert report.medium_count == 1
        assert report.large_count == 1
        
        # Check individual PR sizes
        pr_by_number = {pr.pr_number: pr for pr in report.active_prs}
        assert pr_by_number[1].size == PRSize.SMALL
        assert pr_by_number[2].size == PRSize.MEDIUM
        assert pr_by_number[3].size == PRSize.LARGE
    
    def test_mixed_pr_statuses(self, analyzer, base_date):
        """Test analysis with mixed PR statuses."""
        prs = [
            # Active PR
            PullRequest(
                number=1,
                title="Active feature",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=5),
                updated_at=base_date + timedelta(days=13),  # 2 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="alice",
                labels=["feature"],
                commits=["c1"],
                additions=100,
                deletions=50,
            ),
            # Stale PR
            PullRequest(
                number=2,
                title="Stale bugfix",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=2),  # 13 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="bob",
                labels=["bugfix"],
                commits=["c2"],
                additions=20,
                deletions=10,
            ),
            # Abandoned PR
            PullRequest(
                number=3,
                title="Abandoned refactor",
                state=PRState.OPEN,
                created_at=base_date - timedelta(days=50),
                updated_at=base_date - timedelta(days=40),  # 55 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="charlie",
                labels=["refactor"],
                commits=["c3", "c4", "c5"],
                additions=500,
                deletions=300,
            ),
            # Closed PR (should be ignored)
            PullRequest(
                number=4,
                title="Completed work",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date + timedelta(days=2),
                closed_at=base_date + timedelta(days=2),
                merged_at=base_date + timedelta(days=2),
                merge_commit_sha="merged",
                author="dave",
                labels=[],
                commits=["c6"],
                additions=75,
                deletions=25,
            ),
        ]
        
        report = analyzer.analyze(prs)
        
        assert report.total_open_prs == 3  # Closed PR excluded
        assert report.active_count == 1
        assert report.stale_count == 1
        assert report.abandoned_count == 1
        
        # Check age statistics
        assert report.median_age_days == 14  # Middle of [9, 14, 64]
        assert report.oldest_pr_age_days == 64
    
    def test_recommendations_high_abandoned(self, analyzer, base_date):
        """Test recommendations for high abandoned count."""
        # Create 7 abandoned PRs
        prs = []
        for i in range(7):
            pr = PullRequest(
                number=i+1,
                title=f"Old PR {i+1}",
                state=PRState.OPEN,
                created_at=base_date - timedelta(days=60),
                updated_at=base_date - timedelta(days=35),  # 50 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author=f"dev{i}",
                labels=[],
                commits=[f"commit{i}"],
                additions=100,
                deletions=50,
            )
            prs.append(pr)
        
        report = analyzer.analyze(prs)
        
        assert report.abandoned_count == 7
        assert any("Close or archive 7 abandoned PRs" in rec for rec in report.recommendations)
    
    def test_recommendations_high_stale_percentage(self, analyzer, base_date):
        """Test recommendations for high stale percentage."""
        prs = [
            # 1 active
            PullRequest(
                number=1,
                title="Active",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=10),
                updated_at=base_date + timedelta(days=14),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c1"],
                additions=50,
                deletions=10,
            ),
            # 2 stale (66% stale)
            PullRequest(
                number=2,
                title="Stale 1",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=5),  # 10 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c2"],
                additions=100,
                deletions=50,
            ),
            PullRequest(
                number=3,
                title="Stale 2",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=3),  # 12 days ago
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c3"],
                additions=150,
                deletions=75,
            ),
        ]
        
        report = analyzer.analyze(prs)
        
        assert report.stale_count == 2
        assert report.total_open_prs == 3
        assert any("67% of PRs are stale" in rec for rec in report.recommendations)
    
    def test_recommendations_large_prs(self, analyzer, base_date):
        """Test recommendations for large stale/abandoned PRs."""
        prs = []
        # Create 4 large stale/abandoned PRs
        for i in range(4):
            pr = PullRequest(
                number=i+1,
                title=f"Large PR {i+1}",
                state=PRState.OPEN,
                created_at=base_date - timedelta(days=20),
                updated_at=base_date - timedelta(days=15+i*10),  # Various stale/abandoned
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author=f"dev{i}",
                labels=[],
                commits=[f"c{i}"],
                additions=1000,
                deletions=500,  # Large PR
            )
            prs.append(pr)
        
        report = analyzer.analyze(prs)
        
        assert any("Break down large PRs" in rec for rec in report.recommendations)
    
    def test_recommendations_old_prs(self, analyzer, base_date):
        """Test recommendations for very old PRs."""
        pr = PullRequest(
            number=1,
            title="Ancient PR",
            state=PRState.OPEN,
            created_at=base_date - timedelta(days=100),  # 115 days old
            updated_at=base_date + timedelta(days=14),  # Still active
            closed_at=None,
            merged_at=None,
            merge_commit_sha=None,
            author="dev",
            labels=[],
            commits=["c1"],
            additions=100,
            deletions=50,
        )
        
        report = analyzer.analyze([pr])
        
        assert report.oldest_pr_age_days == 114  # Sept 23 to Jan 15 = 114 days
        assert any("Oldest PR is 114 days old" in rec for rec in report.recommendations)
    
    def test_recommendations_author_concentration(self, analyzer, base_date):
        """Test recommendations when one author has many stale PRs."""
        prs = []
        # Create 5 stale PRs from same author
        for i in range(5):
            pr = PullRequest(
                number=i+1,
                title=f"Bob's PR {i+1}",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=5-i),  # Various stale times
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="bob",
                labels=[],
                commits=[f"c{i}"],
                additions=100,
                deletions=50,
            )
            prs.append(pr)
        
        report = analyzer.analyze(prs)
        
        assert any("bob has 5 stale/abandoned PRs" in rec for rec in report.recommendations)
    
    def test_summary_output(self, analyzer, base_date):
        """Test summary output format."""
        prs = [
            # Active
            PullRequest(
                number=1,
                title="Active",
                state=PRState.OPEN,
                created_at=base_date + timedelta(days=10),
                updated_at=base_date + timedelta(days=14),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c1"],
                additions=50,
                deletions=10,
            ),
            # Stale
            PullRequest(
                number=2,
                title="Stale",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=5),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c2"],
                additions=100,
                deletions=50,
            ),
            # Abandoned
            PullRequest(
                number=3,
                title="Abandoned",
                state=PRState.OPEN,
                created_at=base_date - timedelta(days=50),
                updated_at=base_date - timedelta(days=40),
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="dev",
                labels=[],
                commits=["c3"],
                additions=200,
                deletions=100,
            ),
        ]
        
        report = analyzer.analyze(prs)
        summary = report.get_summary()
        
        assert "Total Open PRs: 3" in summary
        assert "Active: 1 (33%)" in summary
        assert "Stale: 1 (33%)" in summary
        assert "Abandoned: 1 (33%)" in summary
        assert "⚠️  1 PRs need attention" in summary
        assert "❌ 1 PRs should be closed" in summary
    
    def test_detailed_report_output(self, analyzer, base_date):
        """Test detailed report output format."""
        prs = [
            PullRequest(
                number=123,
                title="Feature: Add new dashboard widgets with real-time updates",
                state=PRState.OPEN,
                created_at=base_date,
                updated_at=base_date + timedelta(days=5),  # Stale
                closed_at=None,
                merged_at=None,
                merge_commit_sha=None,
                author="alice",
                labels=["feature"],
                commits=["c1", "c2"],
                additions=250,
                deletions=100,
            ),
        ]
        
        report = analyzer.analyze(prs)
        detailed = report.get_detailed_report()
        
        assert "PR HEALTH REPORT" in detailed
        assert "SUMMARY" in detailed
        assert "SIZE DISTRIBUTION" in detailed
        assert "AGE STATISTICS" in detailed
        assert "STALE PRS (need attention)" in detailed
        assert "PR #123: Feature: Add new dashboard widgets with real-time ..." in detailed
        assert "Author: alice, Size: medium, Inactive: 9 days" in detailed