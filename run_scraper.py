#!/usr/bin/env python3
"""CLI script to run the IRS bulletin scraper."""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_collection.irs_bulletins import IRSBulletinScraper


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_scraper(args) -> None:
    """Run the IRS bulletin scraper.

    Args:
        args: Parsed command line arguments
    """
    scraper = IRSBulletinScraper(
        output_dir=args.output_dir,
        csv_file=args.csv_file,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        max_pages=args.max_pages,
    )

    scraper.run()


def check_bulletins(args) -> None:
    """Check for new bulletins without downloading.

    Args:
        args: Parsed command line arguments
    """
    scraper = IRSBulletinScraper(
        output_dir=args.output_dir,
        csv_file=args.csv_file,
        max_pages=args.check_pages,
    )

    results = scraper.run_check(limit=args.limit, report_file=args.report_file)

    # Print summary to console
    print("\nCheck Results:")
    print(f"  New bulletins found: {results['new_bulletins_count']}")
    print(f"  Total remote checked: {results['total_remote_checked']}")
    print(f"  Total local bulletins: {results['total_local']}")

    if results["new_bulletins"]:
        print("\nNew bulletins available:")
        for filename, _ in results["new_bulletins"]:
            print(f"  • {filename}")


def process_bulletins(args) -> None:
    """Process downloaded bulletins.

    Args:
        args: Parsed command line arguments
    """
    scraper = IRSBulletinScraper(output_dir=args.output_dir)

    if args.validate:
        print("Validating PDF files...")
        validation_results = scraper.validate_pdf_files()
        valid_count = sum(validation_results.values())
        total_count = len(validation_results)
        print(f"Validation complete: {valid_count}/{total_count} files valid")

    if args.cleanup:
        print("Cleaning up invalid files...")
        deleted_files = scraper.cleanup_invalid_files(dry_run=args.dry_run)
        if deleted_files:
            action = "Would delete" if args.dry_run else "Deleted"
            print(f"{action} {len(deleted_files)} invalid files")
        else:
            print("No invalid files found")

    if args.stats:
        print("Generating statistics...")
        stats = scraper.get_bulletin_statistics()
        print("\nBulletin Statistics:")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Total size: {stats['total_size_mb']} MB")
        print(f"  Valid files: {stats['valid_files']}")
        print(f"  Invalid files: {stats['invalid_files']}")
        print(f"  Average size: {stats['average_size_mb']} MB")

    if args.inventory:
        print("Generating inventory report...")
        report = scraper.generate_inventory_report(args.inventory)
        if not args.inventory:
            print(report)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="IRS Bulletin Scraper - Download and manage IRS Internal Revenue Bulletins"
    )

    # Global arguments
    parser.add_argument(
        "--output-dir",
        default="data/raw/irs_bulletins",
        help="Directory to save downloaded PDFs (default: data/raw/irs_bulletins)",
    )
    parser.add_argument(
        "--csv-file",
        default="data/raw/irs_bulletins_summary.csv",
        help="CSV summary file path (default: data/raw/irs_bulletins_summary.csv)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scraper command
    scraper_parser = subparsers.add_parser("download", help="Download bulletins")
    scraper_parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5)",
    )
    scraper_parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries for failed requests (default: 3)",
    )
    scraper_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to scrape (default: unlimited)",
    )

    # Checker command
    checker_parser = subparsers.add_parser("check", help="Check for new bulletins")
    checker_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of recent bulletins to check (default: 20)",
    )
    checker_parser.add_argument("--report-file", help="Save check report to file")
    checker_parser.add_argument(
        "--check-pages",
        type=int,
        default=None,
        help="Maximum number of pages to check (default: unlimited for thorough checking)",
    )

    # Processor command
    processor_parser = subparsers.add_parser(
        "process", help="Process downloaded bulletins"
    )
    processor_parser.add_argument(
        "--validate", action="store_true", help="Validate PDF files"
    )
    processor_parser.add_argument(
        "--cleanup", action="store_true", help="Clean up invalid files"
    )
    processor_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run for cleanup (don't actually delete files)",
    )
    processor_parser.add_argument(
        "--stats", action="store_true", help="Show bulletin statistics"
    )
    processor_parser.add_argument(
        "--inventory",
        nargs="?",
        const="",
        help="Generate inventory report (optionally save to file)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Execute command
    if args.command == "download":
        run_scraper(args)
    elif args.command == "check":
        check_bulletins(args)
    elif args.command == "process":
        process_bulletins(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
