"""Unit tests for data quality validation."""

from datetime import datetime, timedelta, timezone

import pytest

from dora_metrics.calculators.quality import DataQualityReport, DataQualityValidator
from dora_metrics.models import Commit, Deployment, PRState, PullRequest


@pytest.mark.unit
class TestDataQualityValidator:
    """Test data quality validation."""
    
    @pytest.fixture
    def validator(self):
        """Create a data quality validator."""
        return DataQualityValidator()
    
    @pytest.fixture
    def base_date(self):
        """Base date for test data."""
        return datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    def test_empty_data(self, validator):
        """Test validation with no data."""
        report = validator.validate([], [], [])
        
        assert report.total_commits == 0
        assert report.total_prs == 0
        assert report.total_deployments == 0
        assert report.data_quality_score == 1.0  # No data = no issues
        assert not report.has_critical_issues()
        assert not report.has_warnings()
        assert not report.has_informational()
    
    def test_good_data(self, validator, base_date):
        """Test validation with good quality data."""
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Good commit",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
                pr_number=1,
            ),
            Commit(
                sha="commit2",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=1),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=1),
                message="Another good commit",
                files_changed=["file2.py"],
                additions=20,
                deletions=10,
                pr_number=2,
            ),
        ]
        
        prs = [
            PullRequest(
                number=1,
                title="Good PR",
                state=PRState.MERGED,
                created_at=base_date - timedelta(hours=2),
                updated_at=base_date,
                closed_at=base_date,
                merged_at=base_date,
                merge_commit_sha="commit1",
                author="dev",
                labels=[],
                commits=["commit1"],
            ),
            PullRequest(
                number=2,
                title="Another good PR",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date + timedelta(hours=1),
                closed_at=base_date + timedelta(hours=1),
                merged_at=base_date + timedelta(hours=1),
                merge_commit_sha="commit2",
                author="dev",
                labels=[],
                commits=["commit2"],
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Good deployment",
                created_at=base_date + timedelta(hours=2),
                published_at=base_date + timedelta(hours=2),
                commit_sha="commit2",
                is_prerelease=False,
            )
        ]
        
        report = validator.validate(commits, prs, deployments)
        
        assert report.total_commits == 2
        assert report.total_prs == 2
        assert report.total_deployments == 1
        assert report.commits_with_prs == 2
        assert report.commits_without_prs == 0
        assert report.pr_coverage_rate == 1.0  # 100% PR coverage
        assert report.prs_with_commits == 2
        assert report.prs_without_commits == 0
        assert report.pr_completeness_rate == 1.0  # All PRs have valid commits
        assert report.deployments_with_commits == 1
        assert report.deployments_without_commits == 0
        assert report.deployment_commit_rate == 1.0
        assert report.data_quality_score == 1.0  # Perfect score
        assert not report.has_critical_issues()
        assert not report.has_warnings()
        assert report.has_informational()  # PR coverage info
    
    def test_commits_without_prs_high(self, validator, base_date):
        """Test validation with high direct commits (informational)."""
        commits = [
            Commit(
                sha="direct1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Direct push to main",
                files_changed=["hotfix.py"],
                additions=5,
                deletions=2,
            ),
            Commit(
                sha="pr1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=1),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=1),
                message="PR commit",
                files_changed=["feature.py"],
                additions=50,
                deletions=10,
                pr_number=1,
            ),
        ]
        
        prs = [
            PullRequest(
                number=1,
                title="Feature PR",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date + timedelta(hours=1),
                closed_at=base_date + timedelta(hours=1),
                merged_at=base_date + timedelta(hours=1),
                merge_commit_sha="pr1",
                author="dev",
                labels=[],
                commits=["pr1"],
            ),
        ]
        
        report = validator.validate(commits, prs, [])
        
        assert report.commits_with_prs == 1
        assert report.commits_without_prs == 1
        assert report.pr_coverage_rate == 0.5  # 50% PR coverage
        assert not report.has_critical_issues()
        assert not report.has_warnings()  # 50% is above warning threshold
        assert report.has_informational()  # PR coverage is informational
        
        # Check informational content
        pr_info = next(i for i in report.informational if i['type'] == 'pr_coverage')
        assert pr_info['coverage'] == 0.5
        assert pr_info['commits_without_prs'] == 1
    
    def test_commits_without_prs_low(self, validator, base_date):
        """Test validation with very low PR coverage (warning)."""
        commits = [
            Commit(
                sha=f"direct{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=i),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=i),
                message=f"Direct push {i}",
                files_changed=[f"file{i}.py"],
                additions=5,
                deletions=2,
            )
            for i in range(8)  # 8 direct commits
        ] + [
            Commit(
                sha="pr1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=10),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=10),
                message="PR commit",
                files_changed=["feature.py"],
                additions=50,
                deletions=10,
                pr_number=1,
            ),
            Commit(
                sha="pr2",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=11),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=11),
                message="PR commit 2",
                files_changed=["feature2.py"],
                additions=30,
                deletions=5,
                pr_number=2,
            ),
        ]
        
        report = validator.validate(commits, [], [])
        
        assert report.pr_coverage_rate == 0.2  # 20% PR coverage
        assert not report.has_critical_issues()
        assert report.has_warnings()  # Low PR coverage is now a warning
        
        # Check warning content
        pr_warning = next(w for w in report.warnings if w['type'] == 'low_pr_coverage')
        assert pr_warning['coverage'] == 0.2
    
    def test_prs_without_commits(self, validator, base_date):
        """Test validation with PRs referencing missing commits (critical)."""
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Available commit",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
                pr_number=1,
            ),
        ]
        
        prs = [
            PullRequest(
                number=1,
                title="Good PR",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date,
                closed_at=base_date,
                merged_at=base_date,
                merge_commit_sha="commit1",
                author="dev",
                labels=[],
                commits=["commit1"],
            ),
            PullRequest(
                number=2,
                title="PR with missing commits",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date,
                closed_at=base_date,
                merged_at=base_date,
                merge_commit_sha="missing_commit",
                author="dev",
                labels=[],
                commits=["missing_commit", "another_missing"],
            ),
        ]
        
        report = validator.validate(commits, prs, [])
        
        assert report.prs_with_commits == 1
        assert report.prs_without_commits == 1
        assert report.pr_completeness_rate == 0.5
        assert len(report.orphaned_prs) == 1
        assert report.orphaned_prs[0]["pr_number"] == 2
        assert "missing_commit" in report.orphaned_prs[0]["missing_shas"]
        assert report.has_critical_issues()  # PRs with missing commits is now CRITICAL
        assert not report.has_warnings()
        assert report.data_quality_score < 0.6  # Significant penalty for critical issue
        
        # Check critical issues
        pr_critical = [i for i in report.critical_issues if i['type'] == 'pr_missing_reference']
        assert len(pr_critical) == 2  # Two missing commits
        assert all(i['pr_number'] == 2 for i in pr_critical)
    
    def test_deployments_with_missing_commits(self, validator, base_date):
        """Test validation with deployments referencing non-existent commits (critical)."""
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Good commit",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Good deployment",
                created_at=base_date + timedelta(hours=1),
                published_at=base_date + timedelta(hours=1),
                commit_sha="commit1",
                is_prerelease=False,
            ),
            Deployment(
                tag_name="v1.1.0",
                name="Bad deployment",
                created_at=base_date + timedelta(hours=2),
                published_at=base_date + timedelta(hours=2),
                commit_sha="missing_commit",
                is_prerelease=False,
            ),
        ]
        
        report = validator.validate(commits, [], deployments)
        
        assert report.deployments_with_commits == 1
        assert report.deployments_without_commits == 1
        assert report.deployment_commit_rate == 0.5
        assert len(report.critical_issues) == 1
        assert report.critical_issues[0]['type'] == 'missing_reference'
        assert report.critical_issues[0]['deployment'] == "v1.1.0"
        assert report.has_critical_issues()  # This is critical!
        assert report.data_quality_score < 0.6  # Significant penalty
    
    def test_temporal_issues(self, validator, base_date):
        """Test validation with deployments before commits (critical)."""
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=2),  # After deployment!
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=2),
                message="Commit after deployment",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Time traveling deployment",
                created_at=base_date,  # Before commit!
                published_at=base_date,
                commit_sha="commit1",
                is_prerelease=False,
            ),
        ]
        
        report = validator.validate(commits, [], deployments)
        
        assert len(report.critical_issues) == 1
        assert report.critical_issues[0]['type'] == 'temporal'
        assert report.critical_issues[0]['deployment'] == "v1.0.0"
        assert report.critical_issues[0]['time_difference_hours'] == 2.0
        assert report.has_critical_issues()
        assert report.data_quality_score < 0.4  # Major penalty for temporal issues
        
        # Check recommendation about timezone
        assert any("timezone" in rec for rec in report.recommendations)
    
    def test_brief_summary_critical(self, validator, base_date):
        """Test brief summary format for critical issues."""
        commits = [
            Commit(
                sha="commit1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=1),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=1),
                message="Late commit",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
            ),
        ]
        
        deployments = [
            Deployment(
                tag_name="v1.0.0",
                name="Early deployment",
                created_at=base_date,
                published_at=base_date,
                commit_sha="commit1",
                is_prerelease=False,
            ),
            Deployment(
                tag_name="v1.1.0",
                name="Missing commit deployment",
                created_at=base_date + timedelta(hours=2),
                published_at=base_date + timedelta(hours=2),
                commit_sha="missing",
                is_prerelease=False,
            ),
        ]
        
        report = validator.validate(commits, [], deployments)
        summary = report.get_brief_summary()
        
        assert "❌ CRITICAL DATA QUALITY ISSUES:" in summary
        assert "1 deployments occur before their commits" in summary
        assert "v1.0.0: deployed 1.0h before commit" in summary
        assert "1 deployments reference non-existent commits" in summary
        assert "v1.1.0: missing commit" in summary  # SHA is truncated to 8 chars
        assert "Fix these issues or use --force to override" in summary
    
    def test_brief_summary_warnings(self, validator, base_date):
        """Test brief summary format for warnings."""
        commits = [
            Commit(
                sha=f"direct{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=i),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=i),
                message=f"Direct push {i}",
                files_changed=[f"file{i}.py"],
                additions=5,
                deletions=2,
            )
            for i in range(9)  # 9 direct commits
        ] + [
            Commit(
                sha="pr1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=10),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=10),
                message="PR commit",
                files_changed=["feature.py"],
                additions=50,
                deletions=10,
                pr_number=1,
            ),
        ]
        
        report = validator.validate(commits, [], [])
        summary = report.get_brief_summary()
        
        assert "⚠️  Data quality warnings:" in summary
        assert "Only 10% of commits went through PR process" in summary
    
    def test_full_report_output(self, validator, base_date):
        """Test full report format for CLI output."""
        commits = [
            Commit(
                sha="direct1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date,
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date,
                message="Direct push",
                files_changed=["file.py"],
                additions=10,
                deletions=5,
            ),
            Commit(
                sha="pr1",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=1),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=1),
                message="PR commit",
                files_changed=["feature.py"],
                additions=20,
                deletions=10,
                pr_number=1,
            ),
        ]
        
        prs = [
            PullRequest(
                number=1,
                title="Good PR",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date,
                closed_at=base_date,
                merged_at=base_date,
                merge_commit_sha="pr1",
                author="dev",
                labels=[],
                commits=["pr1"],
            ),
            PullRequest(
                number=2,
                title="PR with missing commit",
                state=PRState.MERGED,
                created_at=base_date,
                updated_at=base_date,
                closed_at=base_date,
                merged_at=base_date,
                merge_commit_sha="missing",
                author="dev",
                labels=[],
                commits=["missing"],
            ),
        ]
        
        report = validator.validate(commits, prs, [])
        full_report = report.get_full_report()
        
        assert "DORA METRICS DATA QUALITY REPORT" in full_report
        assert "SUMMARY" in full_report
        assert "Total commits:      2" in full_report
        assert "Total PRs:          2" in full_report
        assert "CRITICAL ISSUES" in full_report  # Now critical, not warning
        assert "1 PRs with Missing References:" in full_report
        assert "PR #2 'PR with missing commit': missing commits" in full_report
        assert "INFORMATIONAL" in full_report
        assert "PR Process Coverage: 50%" in full_report
        assert "RECOMMENDATIONS" in full_report
    
    def test_quality_score_calculation(self, validator, base_date):
        """Test data quality score calculation."""
        # Perfect data - commit with PR
        commits = [Commit(
            sha="c1",
            author_name="Dev",
            author_email="dev@example.com",
            authored_date=base_date,
            committer_name="Dev",
            committer_email="dev@example.com",
            committed_date=base_date,
            message="Good",
            files_changed=["f.py"],
            additions=10,
            deletions=5,
            pr_number=1,
        )]
        
        prs = [PullRequest(
            number=1,
            title="Good PR",
            state=PRState.MERGED,
            created_at=base_date,
            updated_at=base_date,
            closed_at=base_date,
            merged_at=base_date,
            merge_commit_sha="c1",
            author="dev",
            labels=[],
            commits=["c1"],
        )]
        
        report = validator.validate(commits, prs, [])
        assert report.data_quality_score == 1.0
        
        # Add temporal issue - major penalty
        deployments = [Deployment(
            tag_name="v1",
            name="Bad",
            created_at=base_date - timedelta(hours=1),
            published_at=base_date - timedelta(hours=1),
            commit_sha="c1",
            is_prerelease=False,
        )]
        
        report = validator.validate(commits, [], deployments)
        assert report.data_quality_score < 0.4  # Temporal issues cause major penalty
        
        # Test warnings impact
        commits_low_pr = [
            Commit(
                sha=f"c{i}",
                author_name="Dev",
                author_email="dev@example.com",
                authored_date=base_date + timedelta(hours=i),
                committer_name="Dev",
                committer_email="dev@example.com",
                committed_date=base_date + timedelta(hours=i),
                message=f"Commit {i}",
                files_changed=["f.py"],
                additions=10,
                deletions=5,
                pr_number=1 if i == 0 else None,  # Only first has PR
            )
            for i in range(10)
        ]
        
        report = validator.validate(commits_low_pr, [], [])
        assert report.pr_coverage_rate == 0.1  # 10% coverage triggers warning
        assert len(report.warnings) == 1
        assert 0.85 < report.data_quality_score < 0.95  # Minor penalty for warning