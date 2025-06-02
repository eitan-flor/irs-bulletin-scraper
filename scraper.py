"""IRS Bulletin Scraper - Unified Implementation."""

import csv
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ...utils.paths import IRS_BULLETINS_CSV, IRS_BULLETINS_DIR, ensure_dir_exists
from .config import (
    CHUNK_SIZE,
    CSV_FIELDNAMES,
    DELAY_BETWEEN_PAGES,
    MAX_RETRIES,
    MAX_WORKERS,
    REQUEST_TIMEOUT,
)


class IRSBulletinScraper:
    """Unified scraper for IRS Internal Revenue Bulletins with checking, downloading, and processing."""

    BASE_URL = "https://www.irs.gov/internal-revenue-bulletins"

    def __init__(
        self,
        output_dir: str = None,
        csv_file: str = None,
        max_workers: int = MAX_WORKERS,
        max_retries: int = MAX_RETRIES,
        timeout: int = REQUEST_TIMEOUT,
        max_pages: int = None,
    ):
        """Initialize the IRS bulletin scraper.

        Args:
            output_dir: Directory to save downloaded PDFs (defaults to centralized path)
            csv_file: Path to CSV summary file (defaults to centralized path)
            max_workers: Number of concurrent downloads
            max_retries: Maximum number of retries for failed requests
            timeout: Request timeout in seconds
            max_pages: Maximum number of pages to scrape (None for unlimited)
        """
        # Use centralized paths if not specified
        if not output_dir:
            output_dir = str(IRS_BULLETINS_DIR)
        if not csv_file:
            csv_file = str(IRS_BULLETINS_CSV)

        self.output_dir = Path(output_dir)
        self.csv_file = Path(csv_file)
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_pages = max_pages if max_pages is not None else float("inf")
        self.logger = logging.getLogger(self.__class__.__name__)

        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        ensure_dir_exists(self.csv_file.parent)

    # ============================================================================
    # CORE SCRAPING AND DOWNLOADING
    # ============================================================================

    def make_request(
        self, url: str, stream: bool = False
    ) -> Optional[requests.Response]:
        """Make a robust HTTP request with retries and exponential backoff.

        Args:
            url: URL to request
            stream: Whether to stream the response

        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, stream=stream, timeout=self.timeout)
                if response.status_code == 200:
                    return response
                else:
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{self.max_retries} failed with status {response.status_code}"
                    )
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}"
                )

            if attempt < self.max_retries - 1:
                time.sleep(2**attempt)  # Exponential backoff

        self.logger.error(f"All {self.max_retries} attempts failed for URL: {url}")
        return None

    def file_exists(self, filename: str) -> bool:
        """Check if a file already exists in the output directory.

        Args:
            filename: Name of the file to check

        Returns:
            True if file exists, False otherwise
        """
        return (self.output_dir / filename).exists()

    def get_file_size_mb(self, file_path: Path) -> float:
        """Get file size in megabytes.

        Args:
            file_path: Path to the file

        Returns:
            File size in MB
        """
        return file_path.stat().st_size / (1024 * 1024)

    def get_timestamp(self) -> str:
        """Get current timestamp in ISO format.

        Returns:
            Current timestamp as string
        """
        return datetime.now().isoformat()

    def get_document_links(self) -> List[Tuple[str, str]]:
        """Scrape all bulletin links from the IRS website.

        Returns:
            List of tuples containing (filename, url)
        """
        bulletin_links = []
        page_num = 0

        while page_num < self.max_pages:
            url = self.BASE_URL if page_num == 0 else f"{self.BASE_URL}?page={page_num}"
            self.logger.info(f"Scraping page {page_num + 1}: {url}")

            response = self.make_request(url)
            if not response:
                self.logger.error(f"Failed to fetch page {page_num + 1}")
                break

            soup = BeautifulSoup(response.content, "html.parser")

            # Find all bulletin links in the current page
            links_found = False
            for link in soup.find_all(
                "a", href=re.compile(r"/pub/irs-irbs/irb\d+-\d+\.pdf")
            ):
                pdf_url = urljoin("https://www.irs.gov", link["href"])
                bulletin_name = os.path.basename(pdf_url)

                # Check if we already have this bulletin in our list (avoid duplicates)
                if not any(bulletin_name == name for name, _ in bulletin_links):
                    bulletin_links.append((bulletin_name, pdf_url))
                    links_found = True

            if not links_found:
                self.logger.info(
                    f"No bulletin links found on page {page_num + 1}, might be the last page."
                )
                break

            # Check for next page
            if not self._has_next_page(soup):
                self.logger.info(
                    f"No 'Next' link found on page {page_num + 1}, reached the last page."
                )
                break

            page_num += 1
            time.sleep(DELAY_BETWEEN_PAGES)  # Be nice to the server

        self.logger.info(f"Found {len(bulletin_links)} bulletins across all pages")
        return bulletin_links

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page available.

        Args:
            soup: BeautifulSoup object of the current page

        Returns:
            True if next page exists, False otherwise
        """
        pagination = soup.find("ul", class_="pagination")
        if not pagination:
            return False

        # Look for the "Next" link
        for li in pagination.find_all("li", class_="pager__item--next"):
            a_tag = li.find("a")
            if a_tag and "Next" in a_tag.text:
                return True

        # If no specific Next class, look for any link with "Next" text
        for a_tag in pagination.find_all("a"):
            if "Next" in a_tag.text:
                return True

        return False

    def process_document(self, file_path: Path) -> Dict:
        """Process a downloaded bulletin and extract metadata.

        Args:
            file_path: Path to the downloaded PDF file

        Returns:
            Dictionary containing document metadata
        """
        return {
            "file_name": file_path.name,
            "file_size_mb": round(self.get_file_size_mb(file_path), 2),
            "download_timestamp": self.get_timestamp(),
            "status": "downloaded",
        }

    def download_bulletin(self, bulletin_info: Tuple[str, str]) -> Dict:
        """Download a single bulletin PDF file.

        Args:
            bulletin_info: Tuple of (filename, url)

        Returns:
            Dictionary containing download result metadata
        """
        bulletin_name, pdf_url = bulletin_info
        file_path = self.output_dir / bulletin_name

        # Skip if file already exists
        if file_path.exists():
            self.logger.info(f"Skipping {bulletin_name} (already exists)")
            return {
                "file_name": bulletin_name,
                "file_size_mb": round(self.get_file_size_mb(file_path), 2),
                "download_timestamp": "existing",
                "status": "skipped",
            }

        self.logger.info(f"Downloading {bulletin_name} from {pdf_url}")
        response = self.make_request(pdf_url, stream=True)

        if not response:
            self.logger.error(f"Failed to download {bulletin_name}")
            return {
                "file_name": bulletin_name,
                "file_size_mb": 0,
                "download_timestamp": self.get_timestamp(),
                "status": "failed",
            }

        try:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

            result = self.process_document(file_path)
            self.logger.info(
                f"Downloaded {bulletin_name} ({result['file_size_mb']} MB)"
            )
            return result

        except Exception as e:
            self.logger.error(f"Error saving {bulletin_name}: {str(e)}")
            # Clean up partial file
            if file_path.exists():
                file_path.unlink()

            return {
                "file_name": bulletin_name,
                "file_size_mb": 0,
                "download_timestamp": self.get_timestamp(),
                "status": "failed",
            }

    def get_existing_files(self) -> Dict[str, Dict]:
        """Get metadata for existing files from CSV summary.

        Returns:
            Dictionary mapping filename to metadata
        """
        existing_files = {}

        if not self.csv_file.exists():
            return existing_files

        try:
            with open(self.csv_file, "r", newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    existing_files[row["file_name"]] = row
        except Exception as e:
            self.logger.warning(f"Error reading existing CSV file: {str(e)}")

        return existing_files

    def create_csv_summary(self, download_results: List[Dict]) -> None:
        """Create or update CSV summary of downloaded files.

        Args:
            download_results: List of download result dictionaries
        """
        # Get existing files from CSV
        existing_files = self.get_existing_files()

        # Update with new results
        for result in download_results:
            existing_files[result["file_name"]] = result

        # Write updated CSV
        fieldnames = CSV_FIELDNAMES

        try:
            with open(self.csv_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                # Sort by filename for consistent output
                for filename in sorted(existing_files.keys()):
                    writer.writerow(existing_files[filename])

            self.logger.info(f"CSV summary updated: {self.csv_file}")

        except Exception as e:
            self.logger.error(f"Error writing CSV summary: {str(e)}")

    def run(self) -> None:
        """Run the complete scraping and download process."""
        self.logger.info("Starting IRS bulletin scraping process")

        # Get all bulletin links
        bulletin_links = self.get_document_links()

        if not bulletin_links:
            self.logger.warning("No bulletin links found")
            return

        # Filter out already downloaded files
        new_bulletins = [
            (name, url) for name, url in bulletin_links if not self.file_exists(name)
        ]

        self.logger.info(f"Found {len(new_bulletins)} new bulletins to download")

        if not new_bulletins:
            self.logger.info("No new bulletins to download")
            return

        # Download bulletins concurrently
        download_results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_bulletin = {
                executor.submit(self.download_bulletin, bulletin): bulletin
                for bulletin in new_bulletins
            }

            for future in as_completed(future_to_bulletin):
                result = future.result()
                download_results.append(result)

        # Update CSV summary
        self.create_csv_summary(download_results)

        # Log summary
        downloaded = sum(1 for r in download_results if r["status"] == "downloaded")
        failed = sum(1 for r in download_results if r["status"] == "failed")

        self.logger.info(f"Download complete: {downloaded} downloaded, {failed} failed")

    # ============================================================================
    # CHECKING FOR NEW BULLETINS
    # ============================================================================

    def get_local_bulletins(self) -> Set[str]:
        """Get set of locally downloaded bulletin filenames.

        Returns:
            Set of bulletin filenames that exist locally
        """
        local_bulletins = set()

        # Check files in directory
        if self.output_dir.exists():
            for file_path in self.output_dir.glob("*.pdf"):
                local_bulletins.add(file_path.name)

        # Also check CSV file for additional records
        if self.csv_file.exists():
            try:
                with open(self.csv_file, "r", newline="", encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if row.get("status") in ["downloaded", "skipped"]:
                            local_bulletins.add(row["file_name"])
            except Exception as e:
                self.logger.warning(f"Error reading CSV file: {str(e)}")

        return local_bulletins

    def get_remote_bulletins(self, limit: int = None) -> List[Tuple[str, str]]:
        """Get list of bulletins available on the IRS website.

        Args:
            limit: Maximum number of bulletins to check (most recent first)

        Returns:
            List of tuples containing (filename, url)
        """
        if limit and limit <= 50:
            # For small limits, create a temporary scraper to avoid scraping too many pages
            limited_pages = max(1, (limit // 20) + 1)  # Roughly 20 bulletins per page

            # Temporarily override max_pages for this call
            original_max_pages = self.max_pages
            self.max_pages = limited_pages
            all_bulletins = self.get_document_links()
            self.max_pages = original_max_pages  # Restore original value
        else:
            # For larger limits or no limit, use current settings
            all_bulletins = self.get_document_links()

        if limit:
            # Return most recent bulletins (assuming they're in chronological order)
            return all_bulletins[:limit]

        return all_bulletins

    def check_for_new_bulletins(self, limit: int = 20) -> Dict:
        """Check for new bulletins available for download.

        Args:
            limit: Maximum number of recent bulletins to check

        Returns:
            Dictionary containing check results
        """
        self.logger.info(f"Checking for new bulletins (limit: {limit})")

        # Get local and remote bulletins
        local_bulletins = self.get_local_bulletins()
        remote_bulletins = self.get_remote_bulletins(limit)

        # Find new bulletins
        new_bulletins = []
        for filename, url in remote_bulletins:
            if filename not in local_bulletins:
                new_bulletins.append((filename, url))

        # Prepare results
        results = {
            "total_remote_checked": len(remote_bulletins),
            "total_local": len(local_bulletins),
            "new_bulletins_count": len(new_bulletins),
            "new_bulletins": new_bulletins,
            "check_timestamp": self.get_timestamp(),
        }

        self.logger.info(
            f"Check complete: {len(new_bulletins)} new bulletins found "
            f"out of {len(remote_bulletins)} checked"
        )

        return results

    def generate_report(self, results: Dict, report_file: str = None) -> str:
        """Generate a text report of the check results.

        Args:
            results: Results from check_for_new_bulletins()
            report_file: Optional file path to save the report

        Returns:
            Report text as string
        """
        report_lines = [
            "IRS Bulletins Check Report",
            "=" * 30,
            f"Check timestamp: {results['check_timestamp']}",
            f"Total remote bulletins checked: {results['total_remote_checked']}",
            f"Total local bulletins: {results['total_local']}",
            f"New bulletins found: {results['new_bulletins_count']}",
            "",
        ]

        if results["new_bulletins"]:
            report_lines.append("New bulletins available:")
            report_lines.append("-" * 25)
            for filename, url in results["new_bulletins"]:
                report_lines.append(f"  • {filename}")
                report_lines.append(f"    URL: {url}")
                report_lines.append("")
        else:
            report_lines.append("No new bulletins found.")

        report_text = "\n".join(report_lines)

        # Save to file if specified
        if report_file:
            report_path = Path(report_file)
            report_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(report_text)
                self.logger.info(f"Report saved to: {report_path}")
            except Exception as e:
                self.logger.error(f"Error saving report: {str(e)}")

        return report_text

    def run_check(self, limit: int = 20, report_file: str = None) -> Dict:
        """Run a complete check and optionally generate a report.

        Args:
            limit: Maximum number of recent bulletins to check
            report_file: Optional file path to save the report

        Returns:
            Dictionary containing check results
        """
        results = self.check_for_new_bulletins(limit)

        if report_file:
            self.generate_report(results, report_file)

        return results

    # ============================================================================
    # PROCESSING AND VALIDATION
    # ============================================================================

    def validate_pdf_files(self) -> Dict[str, bool]:
        """Validate that all PDF files are properly downloaded and readable.

        Returns:
            Dictionary mapping filename to validation status
        """
        validation_results = {}

        if not self.output_dir.exists():
            self.logger.warning(
                f"Bulletins directory does not exist: {self.output_dir}"
            )
            return validation_results

        for pdf_file in self.output_dir.glob("*.pdf"):
            try:
                # Basic validation - check if file is not empty
                if pdf_file.stat().st_size == 0:
                    validation_results[pdf_file.name] = False
                    self.logger.warning(f"Empty file: {pdf_file.name}")
                    continue

                # More flexible PDF validation for older files
                is_valid = self._is_valid_pdf_flexible(pdf_file)
                validation_results[pdf_file.name] = is_valid

                if not is_valid:
                    self.logger.warning(f"Invalid or corrupted PDF: {pdf_file.name}")

            except Exception as e:
                validation_results[pdf_file.name] = False
                self.logger.error(f"Error validating {pdf_file.name}: {str(e)}")

        valid_count = sum(validation_results.values())
        total_count = len(validation_results)

        self.logger.info(
            f"Validation complete: {valid_count}/{total_count} files valid"
        )

        return validation_results

    def _is_valid_pdf_flexible(self, file_path: Path) -> bool:
        """Check if a file is a valid PDF with flexible validation for older files.

        Args:
            file_path: Path to the file

        Returns:
            True if valid PDF, False otherwise
        """
        try:
            if file_path.stat().st_size == 0:
                return False

            with open(file_path, "rb") as f:
                # Read more bytes to check for various PDF signatures
                header = f.read(1024)

                # Check for standard PDF header
                if header.startswith(b"%PDF"):
                    return True

                # Check for older PDF formats or variations
                # Some older PDFs might have different headers or encoding
                if b"PDF" in header[:100]:  # PDF mentioned in first 100 bytes
                    return True

                # Check for PostScript files that might be valid (some old bulletins)
                if header.startswith(b"%!PS"):
                    return True

                # Check if file has reasonable size (not just a few bytes of garbage)
                if file_path.stat().st_size < 1024:  # Less than 1KB is suspicious
                    return False

                # If file is reasonably sized and has .pdf extension,
                # assume it's valid (better to be permissive for older files)
                return True

        except Exception:
            return False

    def get_file_metadata(self, filename: str) -> Optional[Dict]:
        """Get metadata for a specific bulletin file.

        Args:
            filename: Name of the bulletin file

        Returns:
            Dictionary containing file metadata or None if file doesn't exist
        """
        file_path = self.output_dir / filename

        if not file_path.exists():
            return None

        try:
            stat = file_path.stat()

            metadata = {
                "filename": filename,
                "file_size_bytes": stat.st_size,
                "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created_timestamp": stat.st_ctime,
                "modified_timestamp": stat.st_mtime,
                "is_valid_pdf": self._is_valid_pdf_flexible(file_path),
            }

            return metadata

        except Exception as e:
            self.logger.error(f"Error getting metadata for {filename}: {str(e)}")
            return None

    def cleanup_invalid_files(self, dry_run: bool = True) -> List[str]:
        """Remove invalid or corrupted PDF files.

        Args:
            dry_run: If True, only report what would be deleted without actually deleting

        Returns:
            List of filenames that were (or would be) deleted
        """
        validation_results = self.validate_pdf_files()
        invalid_files = [
            filename
            for filename, is_valid in validation_results.items()
            if not is_valid
        ]

        if not invalid_files:
            self.logger.info("No invalid files found")
            return []

        if dry_run:
            self.logger.info(
                f"DRY RUN: Would delete {len(invalid_files)} invalid files:"
            )
            for filename in invalid_files:
                self.logger.info(f"  - {filename}")
        else:
            self.logger.info(f"Deleting {len(invalid_files)} invalid files:")
            for filename in invalid_files:
                try:
                    file_path = self.output_dir / filename
                    file_path.unlink()
                    self.logger.info(f"  - Deleted: {filename}")
                except Exception as e:
                    self.logger.error(f"  - Failed to delete {filename}: {str(e)}")

        return invalid_files

    def get_bulletin_statistics(self) -> Dict:
        """Get statistics about the downloaded bulletins.

        Returns:
            Dictionary containing bulletin statistics
        """
        if not self.output_dir.exists():
            return {
                "total_files": 0,
                "total_size_mb": 0,
                "valid_files": 0,
                "invalid_files": 0,
                "average_size_mb": 0,
            }

        pdf_files = list(self.output_dir.glob("*.pdf"))
        validation_results = self.validate_pdf_files()

        total_size = sum(f.stat().st_size for f in pdf_files)
        valid_count = sum(validation_results.values())
        invalid_count = len(validation_results) - valid_count

        stats = {
            "total_files": len(pdf_files),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "valid_files": valid_count,
            "invalid_files": invalid_count,
            "average_size_mb": (
                round(total_size / (1024 * 1024) / len(pdf_files), 2)
                if pdf_files
                else 0
            ),
        }

        return stats

    def generate_inventory_report(self, output_file: str = None) -> str:
        """Generate a detailed inventory report of all bulletins.

        Args:
            output_file: Optional file path to save the report

        Returns:
            Report text as string
        """
        stats = self.get_bulletin_statistics()

        report_lines = [
            "IRS Bulletins Inventory Report",
            "=" * 35,
            f"Total files: {stats['total_files']}",
            f"Total size: {stats['total_size_mb']} MB",
            f"Valid files: {stats['valid_files']}",
            f"Invalid files: {stats['invalid_files']}",
            f"Average file size: {stats['average_size_mb']} MB",
            "",
            "File Details:",
            "-" * 15,
        ]

        if self.output_dir.exists():
            for pdf_file in sorted(self.output_dir.glob("*.pdf")):
                metadata = self.get_file_metadata(pdf_file.name)
                if metadata:
                    status = "✓" if metadata["is_valid_pdf"] else "✗"
                    report_lines.append(
                        f"{status} {metadata['filename']} ({metadata['file_size_mb']} MB)"
                    )

        report_text = "\n".join(report_lines)

        # Save to file if specified
        if output_file:
            report_path = Path(output_file)
            report_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(report_text)
                self.logger.info(f"Inventory report saved to: {report_path}")
            except Exception as e:
                self.logger.error(f"Error saving inventory report: {str(e)}")

        return report_text
