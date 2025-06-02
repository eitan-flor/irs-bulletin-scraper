"""Configuration settings for IRS Bulletin Scraper."""

MAX_WORKERS = 5  # Concurrent downloads
MAX_RETRIES = 3  # Number of retry attempts for failed requests
REQUEST_TIMEOUT = 30  # Request timeout in seconds
DELAY_BETWEEN_PAGES = 1  # Seconds to wait between page requests

# Download settings
CHUNK_SIZE = 8192  # Download chunk size in bytes (8KB chunks)

# CSV settings
CSV_FIELDNAMES = ["file_name", "file_size_mb", "download_timestamp", "status"]
