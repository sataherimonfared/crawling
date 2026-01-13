# DESY Website Crawler

A comprehensive web crawler for the DESY website using [Crawl4AI](https://github.com/unclecode/crawl4ai). This script crawls DESY websites with depth control, extracts content as markdown files, and provides detailed logging and error tracking.

## Overview

This crawler uses Breadth-First Search (BFS) to systematically crawl DESY websites, following links from a starting URL to multiple depth levels. It extracts content, preserves tables with links and emails, handles PDFs, and saves everything as organized markdown files.

## Features

- **Deep Crawling**: Follows links automatically using BFS strategy
- **Multi-Depth Support**: Configurable depth levels (0 = root only, 1+ = follow links)
- **Content Extraction**: Extracts HTML content, tables, and preserves links/emails
- **PDF Support**: Handles PDF files with text, image, and table extraction
- **Content Filtering**: Removes navigation, footers, and non-essential content
- **Error Logging**: Comprehensive error tracking with timeout detection
- **Domain Restriction**: Automatically restricts to `*.desy.de` subdomains
- **URL Filtering**: Excludes non-content files (PDFs, images, videos, archives)
- **Performance Optimized**: Configurable concurrency and timeout settings
- **Organized Output**: Saves files by depth level in separate directories

## Requirements

### Python Packages

```bash
pip install crawl4ai beautifulsoup4
```

**Optional (for PDF support):**
```bash
pip install pypdf
```

### System Requirements

- Python 3.8+
- Sufficient disk space for crawled content
- Network access to DESY websites

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install -U crawl4ai beautifulsoup4
   ```
3. (Optional) Install PDF support:
   ```bash
   pip install pypdf
   ```

## Configuration

Edit the configuration section at the top of `crawl_desy_all_urls.py`:

### Basic Settings

```python
# Starting URLs
ROOT_URLS = [
    "https://www.desy.de"
]

# Output directory for markdown files
OUTPUT_DIR = Path("desy_crawled")

# Log directory
LOG_DIR = Path("/home/taheri/crawl4ai/desy_crawled/log")
```

### Crawling Parameters

```python
# Maximum depth to crawl
# 0 = only root page
# 1 = root + pages linked from root
# 2 = root + depth 1 + pages linked from depth 1
MAX_DEPTH = 1

# Maximum total pages to crawl
MAX_PAGES = 200000

# Concurrent tasks (parallelism)
# Higher = faster but uses more resources
CONCURRENT_TASKS = 30
```

### Performance Tuning

```python
# Page timeout (milliseconds)
PAGE_TIMEOUT = 60000  # 60 seconds default
PAGE_TIMEOUT_EXTENDED = 180000  # 180 seconds for problematic URLs
```

### Anti-Bot Settings

```python
ENABLE_STEALTH_MODE = True  # Evade bot detection
HEADLESS = True  # Run browser in headless mode
```

## Usage

### Basic Usage

```bash
python crawl_desy_all_urls.py
```

The crawler will:
1. Start from URLs in `ROOT_URLS`
2. Follow links up to `MAX_DEPTH` levels
3. Save markdown files to `OUTPUT_DIR/depth_N/`
4. Log errors to `LOG_DIR/crawl_errors.json`
5. Generate URL tracking file: `LOG_DIR/urls_by_depth.json`

### Output Structure

```
desy_crawled/
├── depth_0/          # Seed URLs (starting pages)
│   └── desy.de_.md
├── depth_1/          # Pages linked from seed URLs
│   ├── desy.de_index_ger.html.md
│   └── ...
├── depth_2/          # Pages linked from depth 1
│   └── ...
└── log/
    ├── crawl_errors.json      # Failed URLs with error details
    └── urls_by_depth.json     # URL tracking by depth level
```

### File Naming

Markdown files are named based on the URL:
- `https://desy.de/index_ger.html` → `desy.de_index_ger.html.md`
- `https://www.desy.de/about` → `www.desy.de_about.md`

## Output Files

### Markdown Files (`depth_N/*.md`)

Each crawled page is saved as a markdown file containing:
- URL header with source link
- Extracted text content
- Tables with preserved links and emails
- Images (if extracted from PDFs)

### Error Log (`log/crawl_errors.json`)

JSON file containing all failed URLs with:
- Error message and type
- Timeout detection (`is_timeout` flag)
- Timestamp
- Separate `timeout_urls` list for easy retry

Example:
```json
{
  "total_errors": 5,
  "timeout_errors": 3,
  "other_errors": 2,
  "timeout_urls": [
    {
      "url": "https://desy.de/slow-page.html",
      "error": "TimeoutError: Page load timeout",
      "timestamp": "2026-01-12T..."
    }
  ],
  "errors": [...]
}
```

### URL Tracking (`log/urls_by_depth.json`)

JSON file tracking all crawled URLs organized by depth:
- `unique_final_urls`: Deduplicated list of URLs per depth
- `url_entries`: Detailed entries with redirect information
- `link_analysis`: Statistics on links found vs crawled

## Error Handling

### Timeout Errors

URLs that timeout are automatically logged with `is_timeout=True`. To retry with extended timeout:

1. Check `log/crawl_errors.json` for `timeout_urls`
2. Modify the code to use `PAGE_TIMEOUT_EXTENDED` for those specific URLs
3. Re-run the crawler

### Common Errors

- **Timeout**: Page took too long to load → Increase `PAGE_TIMEOUT` or use `PAGE_TIMEOUT_EXTENDED`
- **404 Not Found**: Page doesn't exist → Automatically skipped (not saved)
- **Empty Content**: Page has no meaningful content → Automatically skipped
- **Network Errors**: Connection issues → Logged in `crawl_errors.json`

## Performance Tuning

### Increase Speed

1. **Increase Concurrency**:
   ```python
   CONCURRENT_TASKS = 50  # More parallel requests
   ```

2. **Reduce Timeout** (for faster pages):
   ```python
   PAGE_TIMEOUT = 30000  # 30 seconds
   ```

3. **Limit Depth** (for faster crawling):
   ```python
   MAX_DEPTH = 2  # Crawl fewer levels
   ```

### Reduce Resource Usage

1. **Decrease Concurrency**:
   ```python
   CONCURRENT_TASKS = 10  # Fewer parallel requests
   ```

2. **Increase Timeout** (for stability):
   ```python
   PAGE_TIMEOUT = 120000  # 120 seconds
   ```

## URL Filtering

The crawler automatically excludes non-content file types:
- Images: `.jpg`, `.jpeg`, `.png`
- Documents: `.pdf`, `.docx`, `.xlsx`
- Archives: `.zip`
- Videos: `.mp4`, `.avi`
- Calendar files: `.ics`

These URLs are filtered out before being queued for crawling.

## Domain Restriction

The crawler automatically restricts to DESY domains:
- ✅ Allows: `*.desy.de` (all subdomains)
  - `www.desy.de`
  - `photon-science.desy.de`
  - `particle-physics.desy.de`
  - etc.
- ❌ Excludes: External domains
  - `facebook.com`
  - `instagram.com`
  - `twitter.com`
  - etc.

## Troubleshooting

### Crawler Runs Too Slowly

- Increase `CONCURRENT_TASKS` (default: 30)
- Reduce `PAGE_TIMEOUT` (default: 60000ms)
- Check network connection speed

### Many Timeout Errors

- Increase `PAGE_TIMEOUT` or use `PAGE_TIMEOUT_EXTENDED`
- Check if target website is slow or down
- Review `log/crawl_errors.json` for patterns

### Missing URLs

- Check `log/urls_by_depth.json` to see what was crawled
- Verify `MAX_DEPTH` is sufficient
- Check if URLs are filtered (non-content files, external domains)
- Review error log for failed URLs

### Disk Space Issues

- Reduce `MAX_DEPTH` to crawl fewer pages
- Reduce `MAX_PAGES` to limit total pages
- Clean up old crawl results

## Advanced Features

### Manual Link Extraction

The crawler includes a fallback mechanism that extracts links from HTML (including navigation/footer sections) that might be missed by the main crawler. This ensures comprehensive coverage.

### Redirect Handling

The crawler tracks both original and final (redirected) URLs, ensuring:
- No duplicate crawling of redirected URLs
- Correct depth assignment
- Proper URL tracking in logs

### Empty Page Filtering

Pages with minimal content (< 50 characters or < 10 meaningful words) are automatically skipped to avoid saving empty/error pages.

## License

This script is provided as-is for crawling DESY websites. Ensure you comply with DESY's robots.txt and terms of service when using this crawler.

## Support

For issues or questions:
1. Check `log/crawl_errors.json` for error details
2. Review `log/urls_by_depth.json` for crawl statistics
3. Check console output for warnings and information messages
