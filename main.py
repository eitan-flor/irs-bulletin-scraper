import argparse
import csv
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Constants
OUTPUT_FOLDER = "export"
CSV_SUMMARY_FILE = "export_summary.csv"
BASE_URL = "https://www.irs.gov/internal-revenue-bulletins"
MAX_WORKERS = 8
MAX_RETRIES = 3


def create_output_folder():
    """Create output folder if it doesn't exist."""
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created output folder: {OUTPUT_FOLDER}")


def get_bulletin_links():
    """Scrape all bulletin links from the IRS website."""
    bulletin_links = []
    page_num = 0
    max_pages = 100

    while page_num < max_pages:
        url = BASE_URL if page_num == 0 else f"{BASE_URL}?page={page_num}"
        print(f"Scraping page {page_num + 1}: {url}")

        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    break
                retries += 1
                print(
                    f"Retry {retries}/{MAX_RETRIES} - Status code: {response.status_code}"
                )
                time.sleep(2)
            except Exception as e:
                retries += 1
                print(f"Retry {retries}/{MAX_RETRIES} - Error: {str(e)}")
                time.sleep(2)

        if retries == MAX_RETRIES:
            print(f"Failed to fetch page {page_num + 1} after {MAX_RETRIES} retries.")
            break

        soup = BeautifulSoup(response.content, "html.parser")

        links_found = False
        for link in soup.find_all(
            "a", href=re.compile(r"/pub/irs-irbs/irb\d+-\d+\.pdf")
        ):
            pdf_url = urljoin("https://www.irs.gov", link["href"])
            bulletin_name = os.path.basename(pdf_url)

            if not any(bulletin_name == name for name, _ in bulletin_links):
                bulletin_links.append((bulletin_name, pdf_url))
                links_found = True

        if not links_found:
            print(
                f"No bulletin links found on page {page_num + 1}."
            )
            break

        next_page_link = None
        pagination = soup.find("ul", class_="pagination")

        if pagination:
            for li in pagination.find_all("li", class_="pager__item--next"):
                a_tag = li.find("a")
                if a_tag and "Next" in a_tag.text:
                    next_page_link = a_tag
                    break

            if not next_page_link:
                for a_tag in pagination.find_all("a"):
                    if "Next" in a_tag.text:
                        next_page_link = a_tag
                        break

        if not next_page_link:
            print(
                f"No 'Next' link found on page {page_num + 1}, reached the last page."
            )
            break

        if "href" in next_page_link.attrs:
            next_url = next_page_link["href"]
            parsed_url = urlparse(next_url)
            query_params = parse_qs(parsed_url.query)

            if "page" in query_params:
                try:
                    page_num = int(query_params["page"][0])
                except ValueError:
                    page_num += 1
            else:
                page_num += 1
        else:
            page_num += 1

        print(f"Moving to page {page_num + 1}")
        time.sleep(2)

    print(f"Found {len(bulletin_links)} bulletins across all pages")
    return bulletin_links


def download_bulletin(bulletin_info):
    """Download a bulletin PDF file."""
    bulletin_name, pdf_url = bulletin_info
    file_path = os.path.join(OUTPUT_FOLDER, bulletin_name)

    if os.path.exists(file_path):
        print(f"Skipping {bulletin_name} (already exists)")
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        return {
            "file_name": bulletin_name,
            "file_size_mb": file_size_mb,
            "status": "skipped",
        }

    retries = 0
    while retries < MAX_RETRIES:
        try:
            print(f"Downloading {bulletin_name} from {pdf_url}")
            response = requests.get(pdf_url, stream=True, timeout=30)

            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"Downloaded {bulletin_name} ({file_size_mb:.2f} MB)")
                return {
                    "file_name": bulletin_name,
                    "file_size_mb": file_size_mb,
                    "status": "downloaded",
                }
            else:
                retries += 1
                print(
                    f"Retry {retries}/{MAX_RETRIES} - Status code: {response.status_code}"
                )
                time.sleep(2)
        except Exception as e:
            retries += 1
            print(f"Retry {retries}/{MAX_RETRIES} - Error: {str(e)}")
            time.sleep(2)

    print(f"Failed to download {bulletin_name} after {MAX_RETRIES} retries.")
    return None


def get_existing_files():
    """Get list of existing files and their sizes in the output folder."""
    if not os.path.exists(OUTPUT_FOLDER):
        return []

    existing_files = []
    for filename in os.listdir(OUTPUT_FOLDER):
        if filename.endswith(".pdf"):
            file_path = os.path.join(OUTPUT_FOLDER, filename)
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            existing_files.append(
                {
                    "file_name": filename,
                    "file_size_mb": file_size_mb,
                    "status": "existing",
                }
            )

    return existing_files


def create_csv_summary(download_results):
    """Create a CSV summary of downloaded files."""
    with open(CSV_SUMMARY_FILE, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_name", "file_size_mb", "status"])

        for result in download_results:
            if result:
                writer.writerow(
                    [
                        result["file_name"],
                        f"{result['file_size_mb']:.2f}",
                        result["status"],
                    ]
                )

    print(f"CSV summary created: {CSV_SUMMARY_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Download IRS bulletins")
    parser.add_argument("--output", type=str, help="Output folder for downloaded PDFs")
    parser.add_argument("--csv", type=str, help="CSV summary file name")
    parser.add_argument("--threads", type=int, help="Number of concurrent downloads")
    parser.add_argument(
        "--max-retries", type=int, help="Maximum number of retries for failed requests"
    )
    args = parser.parse_args()

    global OUTPUT_FOLDER, CSV_SUMMARY_FILE, MAX_WORKERS, MAX_RETRIES
    if args.output:
        OUTPUT_FOLDER = args.output
    if args.csv:
        CSV_SUMMARY_FILE = args.csv
    if args.threads:
        MAX_WORKERS = args.threads
    if args.max_retries:
        MAX_RETRIES = args.max_retries

    create_output_folder()

    existing_files = get_existing_files()
    if existing_files:
        print(f"Found {len(existing_files)} existing files in {OUTPUT_FOLDER}")

    bulletin_links = get_bulletin_links()

    if not bulletin_links:
        print("No bulletin links found, exiting.")
        return

    download_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        download_results = list(executor.map(download_bulletin, bulletin_links))

    download_results = [result for result in download_results if result]

    create_csv_summary(download_results)

    downloaded = sum(
        1 for result in download_results if result["status"] == "downloaded"
    )
    skipped = sum(1 for result in download_results if result["status"] == "skipped")

    print(
        f"Downloaded {downloaded} new bulletins, skipped {skipped} existing bulletins"
    )
    print(f"Total: {len(download_results)} bulletins in {OUTPUT_FOLDER}")


if __name__ == "__main__":
    main()
