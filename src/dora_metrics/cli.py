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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        client = GitHubGraphQLClient(token)
        
        # Extract PRs
        click.echo(f"Extracting PRs from {owner}/{repo}...")
        with click.progressbar(
            length=100,
            label='Extracting PRs',
            show_percent=True,
            show_pos=True
        ) as bar:
            prs = []
            
            def pr_callback(pr_batch, total_fetched, estimated_total):
                prs.extend(pr_batch)
                if estimated_total:
                    bar.update(int((total_fetched / estimated_total) * 100) - bar.pos)
            
            client.fetch_pull_requests(
                owner=owner,
                repo=repo,
                since=since_date,
                until=until_date,
                callback=pr_callback
            )
        
        # Extract releases
        click.echo(f"Extracting releases from {owner}/{repo}...")
        with click.progressbar(
            length=100,
            label='Extracting releases',
            show_percent=True,
            show_pos=True
        ) as bar:
            deployments = []
            
            def release_callback(release_batch, total_fetched, estimated_total):
                deployments.extend(release_batch)
                if estimated_total:
                    bar.update(int((total_fetched / estimated_total) * 100) - bar.pos)
            
            client.fetch_releases(
                owner=owner,
                repo=repo,
                since=since_date,
                until=until_date,
                callback=release_callback
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
        
        with click.progressbar(
            commits,
            label='Associating commits',
            show_percent=True,
            show_pos=True
        ) as bar:
            commits = associator.associate_commits_with_prs(list(bar), prs)
        
        commits = associator.identify_deployment_commits(commits, deployments)
        
        # Save updated commits
        ctx.obj.repository.save_commits(repo, commits)
        
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
@click.option('--period', type=click.Choice(['daily', 'weekly', 'monthly']), default='weekly')
@click.option('--since', help='Start date (YYYY-MM-DD)')
@click.option('--until', help='End date (YYYY-MM-DD)')
@click.option('--output-format', type=click.Choice(['json', 'table']), default='table')
@click.pass_context
def calculate(ctx, repo: str, period: str, since: Optional[str], until: Optional[str], output_format: str):
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
        else:  # monthly
            metrics = calculator.calculate_monthly_metrics(commits, deployments, since_date, until_date)
        
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
                    'Lead Time (p50)': f"{period_metrics.lead_time_p50:.1f}h" if period_metrics.lead_time_p50 else "N/A",
                    'Lead Time (p90)': f"{period_metrics.lead_time_p90:.1f}h" if period_metrics.lead_time_p90 else "N/A",
                    'Deploy Freq': f"{period_metrics.deployment_frequency:.1f}/day",
                    'Change Failure %': f"{period_metrics.change_failure_rate:.1%}" if period_metrics.change_failure_rate is not None else "N/A",
                    'MTTR': f"{period_metrics.mttr_hours:.1f}h" if period_metrics.mttr_hours else "N/A"
                }
                data.append(row)
            
            df = pd.DataFrame(data)
            click.echo("\nDORA Metrics Summary")
            click.echo("=" * 80)
            click.echo(df.to_string(index=False))
            
            # Show performance levels
            latest_metrics = list(metrics.values())[-1]
            click.echo("\nPerformance Level (Latest Period):")
            click.echo(f"  Lead Time: {latest_metrics.lead_time_performance}")
            click.echo(f"  Deployment Frequency: {latest_metrics.deployment_frequency_performance}")
            click.echo(f"  Change Failure Rate: {latest_metrics.change_failure_rate_performance}")
            click.echo(f"  MTTR: {latest_metrics.mttr_performance}")
        
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
@click.pass_context
def pr_health(ctx, repo: str, detailed: bool):
    """Analyze PR health and flow efficiency."""
    try:
        # Load PRs
        click.echo("Loading PR data...")
        prs = ctx.obj.repository.load_pull_requests(repo)
        
        # Analyze PR health
        analyzer = PRHealthAnalyzer()
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


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()