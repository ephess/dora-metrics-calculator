"""Data quality validation for DORA metrics."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..models import Commit, Deployment, PullRequest


@dataclass
class DataQualityReport:
    """Report on data quality issues and recommendations."""
    
    # Summary statistics
    total_commits: int = 0
    total_prs: int = 0
    total_deployments: int = 0
    
    # Association metrics (informational)
    commits_with_prs: int = 0
    commits_without_prs: int = 0
    pr_coverage_rate: float = 0.0  # % of commits that went through PRs
    
    prs_with_commits: int = 0
    prs_without_commits: int = 0  # PRs that reference missing commits
    pr_completeness_rate: float = 0.0  # % of PRs that have valid commits
    orphaned_prs: List[Dict] = field(default_factory=list)
    
    deployments_with_commits: int = 0
    deployments_without_commits: int = 0
    deployment_commit_rate: float = 0.0
    orphaned_deployments: List[Dict] = field(default_factory=list)
    
    # Issue categories
    critical_issues: List[Dict] = field(default_factory=list)  # Must fix or tool won't work
    warnings: List[Dict] = field(default_factory=list)  # Should fix but can proceed
    informational: List[Dict] = field(default_factory=list)  # FYI only, shown in full report
    
    # Overall assessment
    data_quality_score: float = 1.0  # 0-1 score
    recommendations: List[str] = field(default_factory=list)
    
    def has_critical_issues(self) -> bool:
        """Check if there are issues that prevent the tool from working."""
        return bool(self.critical_issues)
    
    def has_warnings(self) -> bool:
        """Check if there are issues that should be fixed but don't block."""
        return bool(self.warnings)
    
    def has_informational(self) -> bool:
        """Check if there are informational items."""
        return bool(self.informational)
    
    def get_brief_summary(self) -> str:
        """Get a brief summary for CLI output."""
        lines = []
        
        if self.has_critical_issues():
            lines.append("❌ CRITICAL DATA QUALITY ISSUES:")
            
            # Group critical issues by type
            temporal = [i for i in self.critical_issues if i['type'] == 'temporal']
            missing_deploy = [i for i in self.critical_issues if i['type'] == 'missing_reference']
            missing_pr = [i for i in self.critical_issues if i['type'] == 'pr_missing_reference']
            
            if temporal:
                lines.append(f"  - {len(temporal)} deployments occur before their commits")
                for issue in temporal[:3]:  # Show first 3
                    lines.append(f"    • {issue['deployment']}: deployed {issue['time_difference_hours']:.1f}h before commit")
                if len(temporal) > 3:
                    lines.append(f"    • ... and {len(temporal) - 3} more")
            
            if missing_deploy:
                lines.append(f"  - {len(missing_deploy)} deployments reference non-existent commits")
                for issue in missing_deploy[:3]:
                    lines.append(f"    • {issue['deployment']}: missing commit {issue['missing_sha'][:8]}")
                if len(missing_deploy) > 3:
                    lines.append(f"    • ... and {len(missing_deploy) - 3} more")
            
            if missing_pr:
                # Count unique PRs
                unique_prs = len(set(i['pr_number'] for i in missing_pr))
                lines.append(f"  - {unique_prs} PRs reference non-existent commits")
                shown_prs = set()
                for issue in missing_pr:
                    if issue['pr_number'] not in shown_prs:
                        lines.append(f"    • PR #{issue['pr_number']}: missing commit {issue['missing_sha'][:8]}")
                        shown_prs.add(issue['pr_number'])
                    if len(shown_prs) >= 3:
                        break
                if unique_prs > 3:
                    lines.append(f"    • ... and {unique_prs - 3} more")
            
            lines.append("")
            lines.append("Fix these issues or use --force to override")
            
        elif self.has_warnings():
            lines.append("⚠️  Data quality warnings:")
            
            # Show warnings in a formatted way
            for warning in self.warnings:
                if warning['type'] == 'low_pr_coverage':
                    lines.append(f"  - Only {warning['coverage']:.0%} of commits went through PR process")
                elif warning['type'] == 'pr_missing_commits':
                    lines.append(f"  - {warning['count']} PRs reference non-existent commits")
                elif warning['type'] == 'deployment_missing_commits':
                    lines.append(f"  - {warning['count']} deployments missing commit data")
        
        return "\n".join(lines)
    
    def get_full_report(self) -> str:
        """Get a full detailed report for CLI output."""
        lines = []
        lines.append("=" * 60)
        lines.append("DORA METRICS DATA QUALITY REPORT")
        lines.append("=" * 60)
        lines.append("")
        
        # Summary
        lines.append("SUMMARY")
        lines.append("-" * 20)
        lines.append(f"Total commits:      {self.total_commits}")
        lines.append(f"Total PRs:          {self.total_prs}")
        lines.append(f"Total deployments:  {self.total_deployments}")
        lines.append(f"Data quality score: {self.data_quality_score:.0%}")
        lines.append("")
        
        # Critical Issues
        if self.has_critical_issues():
            lines.append("CRITICAL ISSUES")
            lines.append("-" * 20)
            
            temporal = [i for i in self.critical_issues if i['type'] == 'temporal']
            missing_deploy = [i for i in self.critical_issues if i['type'] == 'missing_reference']
            missing_pr = [i for i in self.critical_issues if i['type'] == 'pr_missing_reference']
            
            if temporal:
                lines.append(f"\n{len(temporal)} Temporal Issues (deployments before commits):")
                for issue in temporal:
                    lines.append(f"  • {issue['deployment']}: {issue['time_difference_hours']:.1f}h before commit")
                    lines.append(f"    Commit: {issue['commit_sha'][:8]} at {issue['commit_time']}")
                    lines.append(f"    Deploy: {issue['deploy_time']}")
            
            if missing_deploy:
                lines.append(f"\n{len(missing_deploy)} Deployment Missing References:")
                for issue in missing_deploy:
                    lines.append(f"  • Deployment '{issue['deployment']}' references non-existent commit: {issue['missing_sha']}")
            
            if missing_pr:
                unique_prs = {}
                for issue in missing_pr:
                    if issue['pr_number'] not in unique_prs:
                        unique_prs[issue['pr_number']] = {
                            'title': issue['pr_title'],
                            'missing_shas': []
                        }
                    unique_prs[issue['pr_number']]['missing_shas'].append(issue['missing_sha'])
                
                lines.append(f"\n{len(unique_prs)} PRs with Missing References:")
                for pr_num, pr_info in unique_prs.items():
                    lines.append(f"  • PR #{pr_num} '{pr_info['title']}': missing commits {', '.join(sha[:8] for sha in pr_info['missing_shas'])}")
            
            lines.append("")
        
        # Warnings
        if self.has_warnings():
            lines.append("WARNINGS")
            lines.append("-" * 20)
            
            for warning in self.warnings:
                if warning['type'] == 'low_pr_coverage':
                    lines.append(f"\nLow PR Coverage: Only {warning['coverage']:.0%} of commits went through PR process")
                    lines.append(f"  • {warning['commits_without_prs']} commits pushed directly to main")
                    lines.append("  • Consider enforcing PR-based workflow for better review process")
                elif warning['type'] == 'deployment_missing_commits':
                    lines.append(f"\n{warning['count']} Deployments with Missing Commits:")
                    for depl in warning['details'][:3]:
                        lines.append(f"  • {depl['deployment']}: missing {depl['missing_sha'][:8]}")
                    if len(warning['details']) > 3:
                        lines.append(f"  • ... and {len(warning['details']) - 3} more")
            
            lines.append("")
        
        # Informational
        if self.informational:
            lines.append("INFORMATIONAL")
            lines.append("-" * 20)
            
            for info in self.informational:
                if info['type'] == 'pr_coverage':
                    lines.append(f"\nPR Process Coverage: {info['coverage']:.0%}")
                    lines.append(f"  • {info['commits_without_prs']} commits pushed directly to main")
                    if info['coverage'] < 0.8:
                        lines.append("  • Consider enforcing PR-based workflow for better review process")
            
            lines.append("")
        
        # Recommendations
        if self.recommendations:
            lines.append("RECOMMENDATIONS")
            lines.append("-" * 20)
            for i, rec in enumerate(self.recommendations, 1):
                lines.append(f"{i}. {rec}")
            lines.append("")
        
        return "\n".join(lines)


class DataQualityValidator:
    """Validates data quality for DORA metrics calculation."""
    
    def __init__(self, min_lead_time_minutes: int = 10):
        """
        Initialize validator.
        
        Args:
            min_lead_time_minutes: Minimum reasonable lead time to not flag as suspicious
        """
        self.min_lead_time_minutes = min_lead_time_minutes
    
    def validate(
        self,
        commits: List[Commit],
        prs: List[PullRequest],
        deployments: List[Deployment]
    ) -> DataQualityReport:
        """
        Validate data quality and generate report.
        
        Args:
            commits: List of commits
            prs: List of pull requests
            deployments: List of deployments
            
        Returns:
            DataQualityReport with findings and recommendations
        """
        report = DataQualityReport()
        
        # Summary statistics
        report.total_commits = len(commits)
        report.total_prs = len(prs)
        report.total_deployments = len(deployments)
        
        # Build lookup structures
        commits_by_sha = {c.sha: c for c in commits}
        prs_by_number = {pr.number: pr for pr in prs}
        
        # Check commit-PR associations
        self._check_pr_associations(commits, prs, commits_by_sha, report)
        
        # Check deployment-commit associations
        self._check_deployment_associations(deployments, commits_by_sha, report)
        
        # Check temporal issues
        self._check_temporal_issues(deployments, commits_by_sha, report)
        
        # Calculate overall score
        report.data_quality_score = self._calculate_quality_score(report)
        
        # Generate recommendations
        self._generate_recommendations(report)
        
        # Standardize issue format for CLI display
        self._standardize_issue_format(report)
        
        return report
    
    def _check_pr_associations(
        self,
        commits: List[Commit],
        prs: List[PullRequest],
        commits_by_sha: Dict[str, Commit],
        report: DataQualityReport
    ) -> None:
        """Check commit-PR associations."""
        # Check commits with/without PRs (for coverage metrics)
        for commit in commits:
            if commit.pr_number:
                report.commits_with_prs += 1
            else:
                report.commits_without_prs += 1
        
        if report.total_commits > 0:
            report.pr_coverage_rate = report.commits_with_prs / report.total_commits
            
            # Add PR coverage - warning if very low, otherwise informational
            if report.pr_coverage_rate < 0.3:  # Less than 30% is concerning
                report.warnings.append({
                    'type': 'low_pr_coverage',
                    'coverage': report.pr_coverage_rate,
                    'commits_without_prs': report.commits_without_prs,
                })
            else:
                report.informational.append({
                    'type': 'pr_coverage',
                    'coverage': report.pr_coverage_rate,
                    'commits_without_prs': report.commits_without_prs,
                    'commits_with_prs': report.commits_with_prs,
                })
        
        # We don't validate PR-to-commit references because:
        # 1. When branches are deleted, their commits disappear from git history
        # 2. This is normal behavior and not actionable
        # So we just count PRs
        report.prs_with_commits = len(prs)
        report.prs_without_commits = 0
        report.orphaned_prs = []
        report.pr_completeness_rate = 1.0
    
    def _check_deployment_associations(
        self,
        deployments: List[Deployment],
        commits_by_sha: Dict[str, Commit],
        report: DataQualityReport
    ) -> None:
        """Check deployment-commit associations."""
        missing_commits = []
        
        for deployment in deployments:
            if deployment.commit_sha in commits_by_sha:
                report.deployments_with_commits += 1
            else:
                report.deployments_without_commits += 1
                missing_commits.append({
                    "deployment": deployment.tag_name,
                    "missing_sha": deployment.commit_sha,
                })
                # This is CRITICAL - deployments must reference valid commits
                report.critical_issues.append({
                    "type": "missing_reference",
                    "deployment": deployment.tag_name,
                    "missing_sha": deployment.commit_sha,
                })
        
        report.orphaned_deployments = missing_commits
        if report.total_deployments > 0:
            report.deployment_commit_rate = report.deployments_with_commits / report.total_deployments
        
        # Add warning if not critical but still some missing
        if missing_commits and report.deployment_commit_rate > 0.5:
            report.warnings.append({
                'type': 'deployment_missing_commits',
                'count': len(missing_commits),
                'details': missing_commits,
            })
    
    def _check_temporal_issues(
        self,
        deployments: List[Deployment],
        commits_by_sha: Dict[str, Commit],
        report: DataQualityReport
    ) -> None:
        """Check for temporal issues like deployments before commits."""
        for deployment in deployments:
            if deployment.commit_sha not in commits_by_sha:
                continue
                
            commit = commits_by_sha[deployment.commit_sha]
            deploy_time = deployment.published_at or deployment.created_at
            
            # Check if deployment happened before commit
            if deploy_time < commit.authored_date:
                time_diff = (commit.authored_date - deploy_time).total_seconds() / 3600
                # This is CRITICAL - impossible timeline
                report.critical_issues.append({
                    "type": "temporal",
                    "deployment": deployment.tag_name,
                    "commit_sha": commit.sha,
                    "deploy_time": deploy_time.isoformat(),
                    "commit_time": commit.authored_date.isoformat(),
                    "time_difference_hours": time_diff,
                })
    
    
    def _calculate_quality_score(self, report: DataQualityReport) -> float:
        """Calculate overall data quality score (0-1)."""
        score = 1.0
        
        # Critical issues severely impact score
        temporal_issues = len([i for i in report.critical_issues if i['type'] == 'temporal'])
        missing_refs = len([i for i in report.critical_issues if i['type'] == 'missing_reference'])
        
        if temporal_issues:
            score *= 0.3  # Major penalty
        if missing_refs:
            score *= 0.5  # Significant penalty
        
        # Warning-level issues have moderate impact
        if report.warnings:
            score *= (0.9 ** len(report.warnings))  # 10% penalty per warning
        
        # Association rates affect score (only if we have data)
        if report.total_prs > 0:
            score *= report.pr_completeness_rate  # PRs with missing commits is bad
        if report.total_deployments > 0:
            score *= report.deployment_commit_rate
        
        # PR coverage is informational only, no penalty
        # (it's a process choice, not a data quality issue)
        
        return max(0.0, min(1.0, score))
    
    def _standardize_issue_format(self, report: DataQualityReport) -> None:
        """Ensure all issues have a consistent format with 'message' field."""
        # Process critical issues
        for issue in report.critical_issues:
            if 'message' not in issue:
                if issue['type'] == 'temporal':
                    issue['message'] = f"Deployment {issue['deployment']} appears to happen before commit {issue['commit_sha'][:8]} (time diff: {issue['time_difference_hours']:.1f} hours)"
                elif issue['type'] == 'missing_reference':
                    issue['message'] = f"Deployment {issue['deployment']} references non-existent commit {issue['missing_sha'][:8]}"
                else:
                    issue['message'] = f"Unknown critical issue type: {issue['type']}"
        
        # Process warnings
        for warning in report.warnings:
            if 'message' not in warning:
                if warning['type'] == 'low_pr_coverage':
                    warning['message'] = f"Low PR coverage: only {warning['coverage']:.1%} of commits went through PRs ({warning['commits_without_prs']} commits without PRs)"
                else:
                    warning['message'] = f"Unknown warning type: {warning['type']}"
        
        # Process informational
        for info in report.informational:
            if 'message' not in info:
                if info['type'] == 'pr_coverage':
                    info['message'] = f"PR coverage: {info['coverage']:.1%} of commits went through PRs ({info['commits_with_prs']} with PRs, {info['commits_without_prs']} without)"
                else:
                    info['message'] = f"Unknown info type: {info['type']}"
    
    def _generate_recommendations(self, report: DataQualityReport) -> None:
        """Generate actionable recommendations based on findings."""
        # Critical issues
        temporal_issues = len([i for i in report.critical_issues if i['type'] == 'temporal'])
        missing_deploy_refs = len([i for i in report.critical_issues if i['type'] == 'missing_reference'])
        
        if temporal_issues:
            report.recommendations.append(
                "Check timezone settings in your CI/CD system - deployments appear to happen before commits"
            )
        
        if missing_deploy_refs:
            report.recommendations.append(
                "Ensure all deployments reference valid commit SHAs - some deployments point to non-existent commits"
            )
        
        # Warnings
        for warning in report.warnings:
            if warning['type'] == 'deployment_missing_commits' and report.deployment_commit_rate < 1.0:
                report.recommendations.append(
                    "Some deployments reference missing commits - ensure full git history is available"
                )
        
        # Informational
        for info in report.informational:
            if info['type'] == 'pr_coverage' and info['coverage'] < 0.5:
                report.recommendations.append(
                    f"Only {info['coverage']:.0%} of commits went through PR review - consider enforcing PR-based workflow for better code quality"
                )