"""Command-line interface for DORA metrics tool."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import pandas as pd

from .analyzers.pr_health import PRHealthAnalyzer
from .calculators.metrics import MetricsCalculator
from .calculators.quality import DataQualityValidator
from .extractors.git_extractor import GitExtractor
from .extractors.github_client import GitHubGraphQLClient
from .models import Commit, Deployment, PullRequest
from .processors.data_associator import DataAssociator
from .storage.csv_handler import CSVHandler
from .storage.repository import DataRepository
from .storage.storage_manager import StorageManager

# Configure logging
from .logging import setup_logging, get_logger

setup_logging(level="INFO")
logger = get_logger(__name__)


class CLIContext:
    """Shared context for CLI commands."""
    
    def __init__(self, storage_path: str):
        self.storage_manager = StorageManager(base_path=Path(storage_path))
        self.repository = DataRepository(self.storage_manager)
        self.csv_handler = CSVHandler()


@click.group()
@click.option(
    '--storage-path',
    default='./dora-data',
    help='Base path for storing DORA metrics data',
    envvar='DORA_STORAGE_PATH'
)
@click.pass_context
def cli(ctx, storage_path: str):
    """DORA metrics back-calculation tool for GitHub repositories."""
    ctx.obj = CLIContext(storage_path)


@cli.command()
@click.option('--repo-path', required=True, help='Path to the git repository')
@click.option('--branch', default='main', help='Branch to analyze')
@click.option('--since', help='Start date (YYYY-MM-DD)')
@click.option('--until', help='End date (YYYY-MM-DD)')
@click.pass_context
def extract_commits(ctx, repo_path: str, branch: str, since: Optional[str], until: Optional[str]):
    """Extract commit data from a local git repository."""
    try:
        # Parse dates
        since_date = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None
        until_date = datetime.fromisoformat(until).replace(tzinfo=timezone.utc) if until else None
        
        # Extract commits
        click.echo(f"Extracting commits from {repo_path} on branch {branch}...")
        extractor = GitExtractor(repo_path)
        
        with click.progressbar(
            length=100,
            label='Extracting commits',
            show_percent=True,
            show_pos=True
        ) as bar:
            commits = extractor.extract_commits(
                branch=branch,
                since=since_date,
                until=until_date,
                progress_callback=lambda pct: bar.update(int(pct * 100) - bar.pos)
            )
        
        # Save commits
        repo_name = Path(repo_path).name
        ctx.obj.repository.save_commits(repo_name, commits)
        
        click.echo(f"✓ Extracted {len(commits)} commits")
        
    except Exception as e:
        click.echo(f"✗ Error extracting commits: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--owner', required=True, help='GitHub repository owner')
@click.option('--repo', required=True, help='GitHub repository name')
@click.option('--token', required=True, help='GitHub personal access token', envvar='GITHUB_TOKEN')
@click.option('--since', help='Start date (YYYY-MM-DD)')
@click.option('--until', help='End date (YYYY-MM-DD)')
@click.pass_context
def extract_github(ctx, owner: str, repo: str, token: str, since: Optional[str], until: Optional[str]):
    """Extract PR and release data from GitHub."""
    try:
        # Parse dates
        since_date = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None
        until_date = datetime.fromisoformat(until).replace(tzinfo=timezone.utc) if until else None
        
        # Create client
        client = GitHubGraphQLClient(token, owner, repo)
        
        # Extract PRs
        click.echo(f"Extracting PRs from {owner}/{repo}...")
        prs = client.fetch_pull_requests(
            since=since_date,
            until=until_date
        )
        
        # Extract releases
        click.echo(f"Extracting releases from {owner}/{repo}...")
        deployments = client.fetch_releases(
            since=since_date,
            until=until_date
        )
        
        # Save data
        ctx.obj.repository.save_pull_requests(repo, prs)
        ctx.obj.repository.save_deployments(repo, deployments)
        
        click.echo(f"✓ Extracted {len(prs)} PRs and {len(deployments)} releases")
        
    except Exception as e:
        click.echo(f"✗ Error extracting GitHub data: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.pass_context
def associate(ctx, repo: str):
    """Associate commits with PRs and deployments."""
    try:
        # Load data
        click.echo("Loading data...")
        commits = ctx.obj.repository.load_commits(repo)
        prs = ctx.obj.repository.load_pull_requests(repo)
        deployments = ctx.obj.repository.load_deployments(repo)
        
        # Associate data
        click.echo("Associating data...")
        associator = DataAssociator()
        
        # Associate all data
        commits, prs = associator.associate_data(commits, prs, deployments)
        
        # Save updated commits and PRs
        ctx.obj.repository.save_commits(repo, commits)
        ctx.obj.repository.save_pull_requests(repo, prs)
        
        # Show summary
        commits_with_prs = sum(1 for c in commits if c.pr_number)
        deployment_commits = sum(1 for c in commits if c.deployment_tag)
        
        click.echo(f"✓ Associated {commits_with_prs}/{len(commits)} commits with PRs")
        click.echo(f"✓ Identified {deployment_commits} deployment commits")
        
    except Exception as e:
        click.echo(f"✗ Error associating data: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.option('--output', required=True, help='Output CSV file path')
@click.pass_context
def export(ctx, repo: str, output: str):
    """Export data to CSV for manual review and annotation."""
    try:
        # Validate data quality first
        click.echo("Validating data quality...")
        commits = ctx.obj.repository.load_commits(repo)
        prs = ctx.obj.repository.load_pull_requests(repo)
        deployments = ctx.obj.repository.load_deployments(repo)
        
        validator = DataQualityValidator()
        report = validator.validate(commits, prs, deployments)
        
        # Show critical issues
        if report.critical_issues:
            click.echo("\n❌ CRITICAL ISSUES (must fix before export):", err=True)
            for issue in report.critical_issues:
                click.echo(f"  - {issue['message']}", err=True)
            click.echo("\nExport blocked due to critical data quality issues.", err=True)
            click.echo("Fix these issues and re-run, or use --force to override.")
            sys.exit(1)
        
        # Show warnings
        if report.warnings:
            click.echo("\n⚠️  WARNINGS (should fix):")
            for warning in report.warnings:
                click.echo(f"  - {warning['message']}")
            click.echo("")
        
        # Export data
        click.echo(f"Exporting data to {output}...")
        output_path = Path(output)
        
        # Export to separate CSV files
        commits_file = output_path.with_suffix('.commits.csv')
        prs_file = output_path.with_suffix('.prs.csv')
        deployments_file = output_path.with_suffix('.deployments.csv')
        
        ctx.obj.csv_handler.export_commits(commits, commits_file)
        ctx.obj.csv_handler.export_pull_requests(prs, prs_file)
        ctx.obj.csv_handler.export_deployments(deployments, deployments_file)
        
        click.echo(f"✓ Exported data to {output}")
        click.echo("\nNext steps:")
        click.echo("1. Review the CSV file")
        click.echo("2. Add manual deployment timestamps where needed")
        click.echo("3. Mark any failed deployments")
        click.echo("4. Import the annotated CSV using 'dora-metrics import'")
        
    except Exception as e:
        click.echo(f"✗ Error exporting data: {e}", err=True)
        sys.exit(1)


@cli.command(name='import')
@click.option('--repo', required=True, help='Repository name')
@click.option('--input', required=True, help='Input CSV file path')
@click.pass_context
def import_csv(ctx, repo: str, input: str):
    """Import annotated CSV data."""
    try:
        # Import data
        click.echo(f"Importing data from {input}...")
        input_path = Path(input)
        
        # Import from separate CSV files
        commits_file = input_path.with_suffix('.commits.csv')
        prs_file = input_path.with_suffix('.prs.csv')
        deployments_file = input_path.with_suffix('.deployments.csv')
        
        commits = ctx.obj.csv_handler.import_commits(commits_file) if commits_file.exists() else []
        prs = ctx.obj.csv_handler.import_pull_requests(prs_file) if prs_file.exists() else []
        deployments = ctx.obj.csv_handler.import_deployments(deployments_file) if deployments_file.exists() else []
        
        # Validate imported data
        validator = DataQualityValidator()
        report = validator.validate(commits, prs, deployments)
        
        # Show validation results
        if report.critical_issues:
            click.echo("\n❌ CRITICAL ISSUES found in imported data:", err=True)
            for issue in report.critical_issues:
                click.echo(f"  - {issue['message']}", err=True)
            click.echo("\nImport blocked due to critical data quality issues.", err=True)
            sys.exit(1)
        
        if report.warnings:
            click.echo("\n⚠️  WARNINGS in imported data:")
            for warning in report.warnings:
                click.echo(f"  - {warning['message']}")
        
        # Save imported data
        ctx.obj.repository.save_commits(repo, commits)
        ctx.obj.repository.save_pull_requests(repo, prs)
        ctx.obj.repository.save_deployments(repo, deployments)
        
        click.echo(f"✓ Imported {len(commits)} commits, {len(prs)} PRs, {len(deployments)} deployments")
        
    except Exception as e:
        click.echo(f"✗ Error importing data: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.option('--period', type=click.Choice(['daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'rolling_7_days', 'rolling_30_days', 'rolling_90_days']), default='weekly')
@click.option('--since', help='Start date (YYYY-MM-DD)')
@click.option('--until', help='End date (YYYY-MM-DD)')
@click.option('--output-format', type=click.Choice(['json', 'table']), default='table')
@click.option('--detailed', is_flag=True, help='Show detailed statistics (p90, etc.)')
@click.pass_context
def calculate(ctx, repo: str, period: str, since: Optional[str], until: Optional[str], output_format: str, detailed: bool):
    """Calculate DORA metrics."""
    try:
        # Load data
        if output_format != 'json':
            click.echo("Loading data...")
        commits = ctx.obj.repository.load_commits(repo)
        deployments = ctx.obj.repository.load_deployments(repo)
        
        # Parse dates
        since_date = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None
        until_date = datetime.fromisoformat(until).replace(tzinfo=timezone.utc) if until else None
        
        # Calculate metrics
        if output_format != 'json':
            click.echo("Calculating metrics...")
        calculator = MetricsCalculator()
        
        if period == 'daily':
            metrics = calculator.calculate_daily_metrics(commits, deployments, since_date, until_date)
        elif period == 'weekly':
            metrics = calculator.calculate_weekly_metrics(commits, deployments, since_date, until_date)
        elif period == 'monthly':
            metrics = calculator.calculate_monthly_metrics(commits, deployments, since_date, until_date)
        elif period == 'quarterly':
            metrics = calculator.calculate_quarterly_metrics(commits, deployments, since_date, until_date)
        elif period == 'yearly':
            metrics = calculator.calculate_yearly_metrics(commits, deployments, since_date, until_date)
        elif period == 'rolling_7_days':
            metrics = calculator.calculate_rolling_7_days_metrics(commits, deployments, since_date, until_date)
        elif period == 'rolling_30_days':
            metrics = calculator.calculate_rolling_30_days_metrics(commits, deployments, since_date, until_date)
        else:  # rolling_90_days
            metrics = calculator.calculate_rolling_90_days_metrics(commits, deployments, since_date, until_date)
        
        # Output results
        if output_format == 'json':
            # JSON output
            output = []
            for period_key, period_metrics in metrics.items():
                output.append({
                    'period': period_key,
                    'metrics': period_metrics.to_dict()
                })
            click.echo(json.dumps(output, indent=2))
        else:
            # Table output
            if not metrics:
                click.echo("No metrics to display for the specified period")
                return
            
            # Convert to DataFrame for nice table display
            data = []
            for period_key, period_metrics in metrics.items():
                row = {
                    'Period': period_key,
                    'Lead Time': _format_lead_time(period_metrics.lead_time_for_changes),
                    'Deploy Freq': _format_deployment_frequency(period_metrics.deployment_frequency),
                    'Change Failure %': f"{period_metrics.change_failure_rate:.1%}" if period_metrics.change_failure_rate is not None else "N/A",
                    'MTTR': _format_mttr(period_metrics.mean_time_to_restore)
                }
                
                # Add detailed stats if requested
                if detailed:
                    row['Lead Time p90'] = _format_lead_time(period_metrics.lead_time_p90)
                    row['Lead Time p95'] = _format_lead_time(period_metrics.lead_time_p95)
                    row['Deploy Count'] = str(period_metrics.deployment_count) if period_metrics.deployment_count else "0"
                    row['Failed Count'] = str(period_metrics.failed_deployment_count) if period_metrics.failed_deployment_count else "0"
                
                data.append(row)
            
            df = pd.DataFrame(data)
            click.echo("\nDORA Metrics Summary")
            click.echo("=" * 80)
            click.echo(df.to_string(index=False))
            
            # Show performance levels (if available)
            if metrics:
                latest_metrics = list(metrics.values())[-1]
                click.echo("\nPerformance Level (Latest Period):")
                click.echo(f"  Lead Time: {_get_lead_time_level(latest_metrics.lead_time_p50)}")
                click.echo(f"  Deployment Frequency: {_get_deployment_frequency_level(latest_metrics.deployment_frequency)}")
                click.echo(f"  Change Failure Rate: {_get_change_failure_rate_level(latest_metrics.change_failure_rate)}")
                click.echo(f"  MTTR: {_get_mttr_level(latest_metrics.mean_time_to_restore)}")
        
    except Exception as e:
        click.echo(f"✗ Error calculating metrics: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.option('--full', is_flag=True, help='Show full quality report')
@click.pass_context
def validate(ctx, repo: str, full: bool):
    """Validate data quality."""
    try:
        # Load data
        click.echo("Loading data...")
        commits = ctx.obj.repository.load_commits(repo)
        prs = ctx.obj.repository.load_pull_requests(repo)
        deployments = ctx.obj.repository.load_deployments(repo)
        
        # Validate
        validator = DataQualityValidator()
        report = validator.validate(commits, prs, deployments)
        
        # Show results
        if report.critical_issues:
            click.echo("\n❌ CRITICAL ISSUES:")
            for issue in report.critical_issues:
                click.echo(f"  - {issue['message']}")
                if issue.get('details'):
                    for detail in issue['details'][:5]:
                        click.echo(f"    • {detail}")
                    if len(issue['details']) > 5:
                        click.echo(f"    • ... and {len(issue['details']) - 5} more")
        
        if report.warnings:
            click.echo("\n⚠️  WARNINGS:")
            for warning in report.warnings:
                click.echo(f"  - {warning['message']}")
                if warning.get('details') and full:
                    for detail in warning['details'][:5]:
                        click.echo(f"    • {detail}")
                    if len(warning['details']) > 5:
                        click.echo(f"    • ... and {len(warning['details']) - 5} more")
        
        if full and report.informational:
            click.echo("\nℹ️  INFORMATIONAL:")
            for info in report.informational:
                click.echo(f"  - {info['message']}")
                if info.get('details'):
                    for detail in info['details'][:5]:
                        click.echo(f"    • {detail}")
                    if len(info['details']) > 5:
                        click.echo(f"    • ... and {len(info['details']) - 5} more")
        
        # Summary
        if not report.critical_issues and not report.warnings:
            click.echo("\n✓ Data quality is good!")
        elif report.critical_issues:
            click.echo("\n⛔ Critical issues must be fixed before calculating metrics")
        else:
            click.echo("\n⚠️  Some warnings found, but you can proceed")
        
    except Exception as e:
        click.echo(f"✗ Error validating data: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.option('--detailed', is_flag=True, help='Show detailed PR health report')
@click.option('--as-of', help='Analyze PR health as of this date (YYYY-MM-DD)')
@click.pass_context
def pr_health(ctx, repo: str, detailed: bool, as_of: Optional[str]):
    """Analyze PR health and flow efficiency."""
    try:
        # Parse reference time if provided
        reference_time = None
        if as_of:
            try:
                reference_time = datetime.fromisoformat(as_of).replace(tzinfo=timezone.utc)
            except ValueError:
                click.echo(f"✗ Invalid date format: {as_of}. Use YYYY-MM-DD", err=True)
                sys.exit(1)
        
        # Load PRs
        click.echo("Loading PR data...")
        prs = ctx.obj.repository.load_pull_requests(repo)
        
        # Analyze PR health
        analyzer = PRHealthAnalyzer(reference_time=reference_time)
        report = analyzer.analyze(prs)
        
        # Show results
        if detailed:
            click.echo(report.get_detailed_report())
        else:
            click.echo(report.get_summary())
            
            if report.recommendations:
                click.echo("\nRECOMMENDATIONS:")
                for i, rec in enumerate(report.recommendations, 1):
                    click.echo(f"{i}. {rec}")
        
    except Exception as e:
        click.echo(f"✗ Error analyzing PR health: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--repo', required=True, help='Repository name')
@click.option('--force', is_flag=True, help='Force update even if no changes detected')
@click.pass_context
def update(ctx, repo: str, force: bool):
    """Update repository data incrementally."""
    try:
        # Load metadata
        metadata = ctx.obj.repository.load_metadata(repo)
        last_update = metadata.get('last_update')
        
        if last_update:
            click.echo(f"Last update: {last_update}")
        else:
            click.echo("No previous update found, performing full extraction...")
            force = True
        
        # TODO: Implement incremental update logic
        # For now, just show a message
        click.echo("⚠️  Incremental updates not yet implemented")
        click.echo("Please run full extraction commands instead")
        
    except Exception as e:
        click.echo(f"✗ Error updating data: {e}", err=True)
        sys.exit(1)


def _format_lead_time(hours: Optional[float]) -> str:
    """Format lead time in a human-readable way."""
    if hours is None:
        return "N/A"
    
    if hours < 24:
        # Less than a day - show in hours
        return f"{hours:.1f}h"
    elif hours < 24 * 7:
        # Less than a week - show in days
        days = hours / 24
        return f"{days:.1f}d"
    elif hours < 24 * 30:
        # Less than a month - show in weeks
        weeks = hours / (24 * 7)
        return f"{weeks:.1f}w"
    else:
        # Show in months
        months = hours / (24 * 30)
        return f"{months:.1f}mo"


def _format_deployment_frequency(freq_per_day: Optional[float]) -> str:
    """Format deployment frequency in a human-readable way."""
    if freq_per_day is None:
        return "N/A"
    
    if freq_per_day >= 1:
        # Multiple per day
        return f"{freq_per_day:.1f}/day"
    elif freq_per_day >= 1/7:
        # Multiple per week
        per_week = freq_per_day * 7
        return f"{per_week:.1f}/week"
    elif freq_per_day >= 1/30:
        # Multiple per month
        per_month = freq_per_day * 30
        return f"{per_month:.1f}/month"
    else:
        # Less than monthly
        per_month = freq_per_day * 30
        return f"{per_month:.2f}/month"


def _format_mttr(hours: Optional[float]) -> str:
    """Format MTTR in a human-readable way."""
    if hours is None:
        return "N/A"
    
    if hours < 1:
        # Less than an hour - show in minutes
        minutes = hours * 60
        return f"{minutes:.0f}m"
    elif hours < 24:
        # Less than a day - show in hours
        return f"{hours:.1f}h"
    elif hours < 24 * 7:
        # Less than a week - show in days
        days = hours / 24
        return f"{days:.1f}d"
    else:
        # Show in weeks
        weeks = hours / (24 * 7)
        return f"{weeks:.1f}w"


def _get_lead_time_level(lead_time_hours: Optional[float]) -> str:
    """Get performance level for lead time."""
    if lead_time_hours is None:
        return "N/A"
    elif lead_time_hours < 24:  # Less than one day
        return "Elite"
    elif lead_time_hours < 168:  # Less than one week
        return "High"
    elif lead_time_hours < 720:  # Less than one month
        return "Medium"
    else:
        return "Low"


def _get_deployment_frequency_level(deploys_per_day: Optional[float]) -> str:
    """Get performance level for deployment frequency."""
    if deploys_per_day is None:
        return "N/A"
    elif deploys_per_day >= 1:  # Multiple deploys per day
        return "Elite"
    elif deploys_per_day >= 1/7:  # At least weekly
        return "High"
    elif deploys_per_day >= 1/30:  # At least monthly
        return "Medium"
    else:
        return "Low"


def _get_change_failure_rate_level(failure_rate: Optional[float]) -> str:
    """Get performance level for change failure rate."""
    if failure_rate is None:
        return "N/A"
    elif failure_rate <= 0.05:  # 5% or less
        return "Elite"
    elif failure_rate <= 0.10:  # 10% or less
        return "High"
    elif failure_rate <= 0.15:  # 15% or less
        return "Medium"
    else:
        return "Low"


def _get_mttr_level(mttr_hours: Optional[float]) -> str:
    """Get performance level for MTTR."""
    if mttr_hours is None:
        return "N/A"
    elif mttr_hours < 1:  # Less than one hour
        return "Elite"
    elif mttr_hours < 24:  # Less than one day
        return "High"
    elif mttr_hours < 168:  # Less than one week
        return "Medium"
    else:
        return "Low"


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()