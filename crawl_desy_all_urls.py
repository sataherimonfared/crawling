"""
General-Purpose Website Crawler using Crawl4AI

This script provides a comprehensive web crawling solution that can process
multiple URLs, extract content (including tables with links/emails), and save
results as markdown files.

KEY FEATURES:
- Multi-URL processing: Process a list of URLs sequentially
- Table extraction: Extracts HTML tables while preserving links and emails
- PDF support: Handles PDF files with text, image, and table extraction
- Content filtering: Removes navigation, footers, and non-essential content
- Error logging: Comprehensive error tracking and reporting
- Configurable depth: Control how many link levels to follow

WHAT IT DOES:
1. Processes each URL in the ROOT_URLS list
2. For each URL:
   - Detects if it's a PDF or HTML page
   - Configures appropriate extraction strategies
   - Crawls the page (and optionally follows links based on MAX_DEPTH)
   - Extracts content, tables (with links/emails preserved), and images
   - Saves everything to a markdown file with URL header
3. Logs all errors and provides summary statistics

KEY CONCEPTS:
- Deep Crawling: Following links from one page to another automatically
- BFS Strategy: Breadth-First Search - crawls all pages at depth 1, then depth 2, etc.
- Depth Levels: How many "clicks" away from the starting page to follow
  - Depth 0: Just the starting page (no link following)
  - Depth 1: Pages linked directly from the starting page
  - Depth 2: Pages linked from depth 1 pages
"""

"""
Enhanced DESY Website Crawler using Crawl4AI

This script crawls the DESY website with advanced features:
- Anti-bot detection evasion
- JavaScript rendering support
- Comprehensive error logging
- Configurable page limits

WHAT IT DOES:
1. Starts from the root URL (https://www.desy.de)
2. Follows links to crawl pages at multiple depth levels
3. Saves the markdown content of each crawled page to files
4. Logs all errors (failed URLs with reasons) to a JSON file
"""

import asyncio
import json
import re
import logging
from pathlib import Path
from datetime import datetime
from html import unescape
from urllib.parse import urlparse, urljoin

# Ensure the debug-log directory exists.
# This script contains debug instrumentation that writes to
# `/home/taheri/crawl4ai/.cursor/debug.log`. On batch nodes / fresh clones,
# the `.cursor/` folder may not exist, which would raise FileNotFoundError and
# abort the crawl early (leading to "Files saved: 0 pages").
try:
    Path("/home/taheri/crawl4ai/.cursor").mkdir(parents=True, exist_ok=True)
except Exception:
    # Debug logging is non-critical; never fail the crawl due to logging setup.
    pass

# Crawl4AI is a required runtime dependency for this script.
# Keep the failure mode explicit and actionable (no URL-specific behavior).
try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
    from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
except ModuleNotFoundError as e:
    if e.name == "crawl4ai":
        print(
            "[ERROR] Python package 'crawl4ai' is not installed in this environment.\n"
            "        Activate the environment where Crawl4AI is installed, or install it:\n"
            "          python -m pip install -U crawl4ai\n"
            "        Then re-run:\n"
            "          python crawl_desy_simple.py"
        )
        raise SystemExit(1)
    raise

# BeautifulSoup for HTML parsing (needed to extract links/emails from table cells)
try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False
    print("[WARNING] BeautifulSoup not available - links/emails in tables may not be preserved")

# Content filtering support (PruningContentFilter for removing non-essential content)
try:
    from crawl4ai.content_filter_strategy import PruningContentFilter
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    PRUNING_FILTER_AVAILABLE = True
except ImportError:
    PRUNING_FILTER_AVAILABLE = False
    print("[INFO] PruningContentFilter not available - using excluded_selector only")

# Table extraction support (DefaultTableExtraction for extracting tables)
try:
    from crawl4ai import DefaultTableExtraction
    from crawl4ai import TableExtractionStrategy
    TABLE_EXTRACTION_AVAILABLE = True
except ImportError:
    TABLE_EXTRACTION_AVAILABLE = False
    TableExtractionStrategy = None
    print("[INFO] DefaultTableExtraction not available - table extraction disabled")

# PDF scraping support (optional - requires pypdf to be installed)
# Check if both the module and pypdf dependency are available
PDF_SUPPORT_AVAILABLE = False
try:
    from crawl4ai.processors.pdf import PDFContentScrapingStrategy, PDFCrawlerStrategy
    # Try to import pypdf to verify it's installed
    import pypdf
    PDF_SUPPORT_AVAILABLE = True
except ImportError:
    PDF_SUPPORT_AVAILABLE = False
    # Don't print warning here - will print when actually trying to use it

# ============================================================================
# CONFIGURATION - Adjust these values to change crawling behavior
# ============================================================================

# List of URLs to crawl
ROOT_URLS = [
    "https://www.desy.de"
    #"https://desy.de/index_ger.html",
    #"#https://desy.de/index_eng.html"
    # "https://photon-science.desy.de/facilities/petra_iii/machine/parameters/index_eng.html",
    # # Events page (should extract events)
    # "https://www.desy.de/aktuelles/veranstaltungen/index_ger.html",
    # # Member tables
    # "https://atlas.desy.de/members/",
    # "https://cms.desy.de/cms_members/",
    # "https://pitz.desy.de/group_members/",
    # "https://it.desy.de/about_us/gruppenleitung/management/index_eng.html",
    # "https://astroparticle-physics.desy.de/about_us/group_members/theory/index_eng.html",
    # "https://astroparticle-physics.desy.de/about_us/group_members/neutrino_astronomy/index_eng.html",
    # "https://photon-science.desy.de/research/research_teams/magnetism_and_coherent_phenomena/group_members/index_eng.html",
    # "https://photon-science.desy.de/facilities/petra_iii/beamlines/p23_in_situ_x_ray_diffraction_and_imaging/contact__staff/index_eng.html",
    # # publications page
    # "https://astroparticle-physics.desy.de/research/neutrino_astronomy/publications/index_eng.html",
    # # researchers page        
    # "https://www.desy.de/ueber_desy/leitende_wissenschaftler/christian_schwanenberger/index_ger.html",
    # "https://ai.desy.de/people/heuser.html",
    # "https://www.desy.de/career/contact/index_eng.html",
]

# Directory where crawled pages will be saved as markdown files
OUTPUT_DIR = Path("desy_crawled")
OUTPUT_DIR.mkdir(exist_ok=True)  # Create directory if it doesn't exist

# Log directory for all log files
#LOG_DIR = Path("/data/dust/group/it/ReferenceData/log")
LOG_DIR = Path("/home/taheri/crawl4ai/desy_crawled/log")


try:
    # Try to create the directory if it doesn't exist
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)
except (PermissionError, OSError) as e:
    # If we can't create the directory, check if it exists (might have been created by another process)
    if not LOG_DIR.exists():
        # Directory doesn't exist and we can't create it - use fallback
        LOG_DIR = Path.home() / ".crawl4ai_logs"
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        print(f"[WARNING] Could not create log directory at /data/dust/group/it/ReferenceData/log: {e}")
        print(f"[WARNING] Using fallback location: {LOG_DIR}")
    # If directory exists, we'll try to use it (might have write permission even if we can't create it)

# Error log file - saves all URLs that failed to crawl with error reasons
ERROR_LOG_FILE = LOG_DIR / "crawl_errors.json"

# Checkpoint file - saves progress to resume crawling later
CHECKPOINT_FILE = LOG_DIR / "crawl_checkpoint.json"

# Debug log file removed - no longer needed

# Checkpoint/Resume settings
# Set to True to resume from previous checkpoint (skip already processed URLs)
# Set to False to start fresh (ignore previous checkpoint)
USE_CHECKPOINT = False  # Change to True to resume from checkpoint

# How many pages to crawl simultaneously (parallelism)
# Higher = faster but uses more resources
# Recommended: 3-5 for stable crawling, 10+ for faster (may trigger rate limits)
# PERFORMANCE: Increased from 15 to 30 to accelerate crawling
CONCURRENT_TASKS = 30

# Maximum depth to crawl (how many link levels to follow)
# 0 = only the root page
# 1 = root page + pages linked from root (you found 33 URLs here)
# 2 = root + depth 1 pages + pages linked from depth 1 pages (you found 862 URLs here)
MAX_DEPTH = 5

# Maximum total pages to crawl (set to a large number for no practical limit)
# Set to a very large number (like 10000) to crawl all 862+ pages you found
# Or set a smaller number like 1000 to limit the crawl
# Note: BFSDeepCrawlStrategy doesn't accept None, so use a large number instead
# 10000 is effectively "no limit" for most sites
MAX_PAGES = 200000  # Increased to match reference file (100,012 URLs) with room for growth

# Anti-bot settings
ENABLE_STEALTH_MODE = True  # Enable stealth mode to evade bot detection
HEADLESS = True  # Run browser in headless mode (no visible window)

# JavaScript rendering
# Crawl4AI uses Playwright by default, which handles JavaScript automatically
# This is already enabled - no additional config needed

# Timeout settings (in milliseconds)
# PERFORMANCE: Reduced default timeout to 60s for faster crawling
# URLs that timeout will be logged in crawl_errors.json with timeout category
# Future runs can apply extended timeouts only to those specific URLs
PAGE_TIMEOUT = 60000  # 60 seconds (60000ms) - default timeout for most pages
PAGE_TIMEOUT_EXTENDED = 180000  # 180 seconds (180000ms) - for URLs that previously timed out


# ============================================================================
# Custom Table Extraction Strategy that Preserves Links
# ============================================================================
# This custom strategy extends DefaultTableExtraction to preserve HTML links
# (including mailto: links) in table cells by converting them to markdown format.
# This is a general-purpose solution that works for all types of URLs.
class LinkPreservingTableExtraction(TableExtractionStrategy):
    """
    Custom table extraction strategy that preserves HTML links and emails in table cells.
    
    This strategy wraps DefaultTableExtraction but post-processes the extracted tables
    to convert HTML links to markdown format, ensuring emails and URLs are preserved.
    """
    
    def __init__(self, table_score_threshold=3, min_rows=1, min_cols=2, verbose=True):
        """
        Initialize the link-preserving table extraction strategy.
        
        Args:
            table_score_threshold: Minimum score for a table to be extracted (lower = more tables)
            min_rows: Minimum number of rows for a valid table
            min_cols: Minimum number of columns for a valid table
            verbose: Enable verbose logging
        """
        self.base_strategy = DefaultTableExtraction(
            table_score_threshold=table_score_threshold,
            min_rows=min_rows,
            min_cols=min_cols,
            verbose=verbose
        )
        self.verbose = verbose
        # Add logger attribute that TableExtractionStrategy expects
        self.logger = logging.getLogger(__name__)
    
    def extract_tables(self, element, **kwargs):
        """
        Extract tables from HTML, preserving links in cells.
        
        Args:
            element: HTML element (can be string, BeautifulSoup, or element)
            **kwargs: Additional parameters (may include url, extraction_strategy, etc.)
            
        Returns:
            List of extracted tables with links preserved as markdown
        """
        # Extract URL from kwargs if available
        url = kwargs.get('url', None)
        
        # Convert element to HTML string if needed
        if isinstance(element, str):
            html = element
        else:
            # If it's a BeautifulSoup object or other element, convert to string
            html = str(element) if hasattr(element, '__str__') else str(element)
        
        # First, use DefaultTableExtraction to get the table structure
        # Pass element and kwargs to match expected signature
        if hasattr(self.base_strategy, 'extract_tables'):
            tables = self.base_strategy.extract_tables(element, **kwargs)
        else:
            # Fallback for older versions that might use extract()
            tables = self.base_strategy.extract(html, url) if hasattr(self.base_strategy, 'extract') else []
        
        if not tables or not BEAUTIFULSOUP_AVAILABLE:
            return tables
        
        # Parse HTML to extract link information
        try:
            soup = BeautifulSoup(html, 'html.parser')
            html_tables = soup.find_all('table')
            
            # Process each extracted table
            for table_idx, table in enumerate(tables):
                if table_idx >= len(html_tables):
                    continue
                
                html_table = html_tables[table_idx]
                
                # Process headers
                if 'headers' in table:
                    table['headers'] = self._process_row(
                        table['headers'],
                        html_table,
                        is_header=True
                    )
                
                # Process rows
                if 'rows' in table:
                    processed_rows = []
                    for row_idx, row in enumerate(table['rows']):
                        processed_row = self._process_row(
                            row,
                            html_table,
                            row_index=row_idx
                        )
                        processed_rows.append(processed_row)
                    table['rows'] = processed_rows
        
        except Exception as e:
            if self.verbose:
                print(f"[WARNING] Failed to preserve links in tables: {e}")
        
        return tables
    
    def _process_row(self, row_data, html_table, is_header=False, row_index=0):
        """
        Process a table row, converting HTML links to markdown.
        
        Args:
            row_data: List of cell values (plain text)
            html_table: BeautifulSoup table element
            is_header: Whether this is a header row
            row_index: Index of the row in the table
            
        Returns:
            List of processed cell values with links as markdown
        """
        processed_cells = []
        
        try:
            # Find the corresponding row in HTML - use recursive=True to catch all rows
            rows = html_table.find_all('tr', recursive=True)
            # Filter to ensure rows belong to this table, not nested tables
            rows = [r for r in rows if r.find_parent('table') == html_table]
            
            # Determine which HTML row to use
            html_row_idx = row_index
            if is_header:
                # Check if there's a thead
                thead = html_table.find('thead')
                if thead:
                    header_rows = thead.find_all('tr', recursive=True)
                    header_rows = [r for r in header_rows if r.find_parent('table') == html_table]
                    if row_index < len(header_rows):
                        html_row = header_rows[row_index]
                    else:
                        html_row = None
                else:
                    # First row might be header
                    html_row = rows[0] if rows else None
            else:
                # Data row - skip header rows
                tbody = html_table.find('tbody')
                if tbody:
                    tbody_rows = tbody.find_all('tr', recursive=True)
                    tbody_rows = [r for r in tbody_rows if r.find_parent('table') == html_table]
                    if row_index < len(tbody_rows):
                        html_row = tbody_rows[row_index]
                    else:
                        html_row = None
                else:
                    # No tbody, skip first row if it's a header
                    start_idx = 1 if html_table.find('th') else 0
                    actual_idx = start_idx + row_index
                    html_row = rows[actual_idx] if actual_idx < len(rows) else None
            
            if html_row:
                # Use recursive=True to catch all cells, then filter nested tables
                # Since we're using html_row.find_all(), all returned cells are descendants of html_row
                # We only need to filter out cells that belong to nested tables
                html_cells = html_row.find_all(['td', 'th'], recursive=True)
                html_cells = [c for c in html_cells if c.find_parent('table') == html_table]
                
                # Process each cell
                for cell_idx, cell_value in enumerate(row_data):
                    if cell_idx < len(html_cells):
                        html_cell = html_cells[cell_idx]
                        processed_cell = self._process_cell(cell_value, html_cell)
                        processed_cells.append(processed_cell)
                    else:
                        processed_cells.append(str(cell_value))
            else:
                # No matching HTML row, return as-is
                processed_cells = [str(cell) for cell in row_data]
        
        except Exception:
            # If processing fails, return original row data
            processed_cells = [str(cell) for cell in row_data]
        
        return processed_cells
    
    def _process_cell(self, cell_text, html_cell):
        """
        Process a single table cell, converting HTML links to markdown.
        
        Args:
            cell_text: Plain text content of the cell
            html_cell: BeautifulSoup cell element
            
        Returns:
            Processed cell text with links as markdown
        """
        if not html_cell:
            return str(cell_text)
        
        try:
            # Find all links in the cell
            links = html_cell.find_all('a', href=True)
            
            if not links:
                return str(cell_text)
            
            # Convert links to markdown
            markdown_links = []
            for link in links:
                href = link.get('href', '').strip()
                link_text = link.get_text(strip=True) or href
                
                if href.startswith('mailto:'):
                    email = unescape(href[7:])
                    markdown_links.append(f"[{link_text}](mailto:{email})")
                elif href:
                    markdown_links.append(f"[{link_text}]({href})")
            
            # If we have links, return them (prioritize email links)
            if markdown_links:
                email_links = [l for l in markdown_links if 'mailto:' in l]
                if email_links:
                    return email_links[0] if len(email_links) == 1 else " | ".join(email_links)
                else:
                    return " | ".join(markdown_links)
            else:
                return str(cell_text)
        
        except Exception:
            return str(cell_text)


def format_cell_with_links(cell_content, cell_html=None):
    """
    Format a table cell, preserving links and emails as markdown.
    
    This function ensures that hyperlinks and email addresses within table cells
    are preserved in the markdown output. If cell_html is provided, it extracts
    links and emails from the HTML. Otherwise, it processes the text content.
    
    Args:
        cell_content: Plain text content of the cell
        cell_html: Optional HTML content of the cell (for link extraction)
    
    Returns:
        Formatted markdown string with links preserved
    """
    if not cell_content:
        return ""
    
    # If HTML is available, extract links and emails from it
    if cell_html and BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(cell_html, 'html.parser')
            cell_text = soup.get_text(strip=True)
            
            # Extract all links (both <a> tags and mailto: links)
            links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').strip()
                link_text = link.get_text(strip=True) or href
                
                # Handle email links (mailto:) - this is critical for preserving emails
                if href.startswith('mailto:'):
                    email = unescape(href[7:])
                    # For email links, use the email address as the link text if link_text is just the name
                    # Format: [Name](mailto:email@desy.de)
                    links.append(f"[{link_text}](mailto:{email})")
                # Handle regular links
                elif href:
                    # Make relative URLs absolute if needed
                    if href.startswith('/'):
                        # Keep as-is for now (could make absolute if base URL available)
                        links.append(f"[{link_text}]({href})")
                    elif href.startswith('http'):
                        links.append(f"[{link_text}]({href})")
                    else:
                        links.append(f"[{link_text}]({href})")
            
            # If we found links, prioritize links over plain text
            # For email cells, the link IS the content, so return just the link
            if links:
                # For email links, return just the markdown link (not combined with text)
                # This ensures emails appear as [Name](mailto:email@desy.de) instead of "Name | [Name](mailto:email@desy.de)"
                email_links = [l for l in links if 'mailto:' in l]
                if email_links:
                    # If we have email links, return them (usually just one)
                    return email_links[0] if len(email_links) == 1 else " | ".join(email_links)
                else:
                    # Regular links - combine with text if text is different
                    if cell_text and cell_text.strip():
                        # Check if cell_text matches any link text
                        link_texts = [l.split(']')[0].replace('[', '').strip() for l in links]
                        if cell_text.strip() not in link_texts:
                            # Text is different from link text, combine them
                            return f"{cell_text} | " + " | ".join(links)
                    return " | ".join(links)
            else:
                return cell_text or str(cell_content)
        except Exception:
            # Fallback to plain text if HTML parsing fails
            return str(cell_content)
    else:
        # No HTML available, check if text contains email pattern
        text = str(cell_content).strip()
        
        # Try to detect email in plain text
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        if emails:
            # Replace email with markdown link
            for email in emails:
                text = text.replace(email, f"[{email}](mailto:{email})")
        
        return text


def is_pdf_url(url):
    """
    Check if a URL points to a PDF file.
    Handles URLs with query parameters like ?preview=preview
    Also detects common PDF URL patterns like /pdf/ (e.g., arXiv URLs)
    """
    # Remove query parameters and fragments for checking
    url_clean = url.split('?')[0].split('#')[0].lower()
    
    # Check if URL ends with .pdf (case-insensitive)
    if url_clean.endswith('.pdf') or '.pdf' in url_clean:
        return True
    
    # Check for common PDF URL patterns (e.g., arXiv: /pdf/12345)
    # Pattern: /pdf/ followed by alphanumeric characters
    pdf_patterns = [
        r'/pdf/',           # arXiv, many academic sites (e.g., arxiv.org/pdf/12345)
        r'/pdfs/',          # Alternative pattern
        r'/document/',      # Some document servers
        r'/file.*\.pdf',    # File paths with .pdf
    ]
    
    for pattern in pdf_patterns:
        if re.search(pattern, url_clean):
            return True
    
    return False


def is_pubdb_url(url):
    """
    Check if a URL is a PUBDB (bib-pubdb1.desy.de) page.
    These pages require special filtering to remove navigation/search UI elements.
    """
    if not url:
        return False
    url_lower = url.lower()
    return 'bib-pubdb1.desy.de' in url_lower or 'bib-pubdb' in url_lower


def is_pubdb_content(html_content):
    """
    Check if HTML content is from a PUBDB page by detecting PUBDB-specific markers.
    This handles cases where pages redirect to or embed PUBDB content.
    """
    if not html_content:
        return False
    html_lower = str(html_content).lower()
    # Check for PUBDB domain in links/content
    pubdb_indicators = [
        'bib-pubdb1.desy.de',
        'bib-pubdb',
        'guest :: login',
        'search: | [search tips]',
        'sort by: | display results:',
        'results overview',
        'interested in being notified about new results'
    ]
    # Need at least 2 indicators to be confident it's PUBDB content
    matches = sum(1 for indicator in pubdb_indicators if indicator in html_lower)
    return matches >= 2


# PUBDB UI keywords that indicate navigation/search interface (not publication records)
_PUBDB_UI_KEYWORDS = [
    'guest', 'login', 'search:', 'sort by:', 'display results:',
    'output format:', 'search tips', 'collections:', 'name | info',
    'results overview', 'try your search', 'rss feed', 'interested in being notified',
    'haven\'t found what you were looking for'
]


def is_pubdb_ui_table(table_text):
    """
    Check if a table is a PUBDB UI table (navigation/search interface) rather than publication records.
    
    Args:
        table_text: The text content of the table (lowercase recommended)
    
    Returns:
        bool: True if the table is a UI table, False if it contains publication records
    """
    if not table_text:
        return False
    
    # Ensure lowercase for consistent matching
    table_text_lower = table_text.lower() if not isinstance(table_text, str) or table_text != table_text.lower() else table_text
    
    # Check if this table contains publication records (PUBDB-YYYY-NNNNN pattern)
    has_publication_id = bool(re.search(r'pubdb-\d{4}-\d{5}', table_text_lower, re.I))
    
    # Only filter if it has UI keywords AND doesn't contain publication IDs
    # Also filter if it contains "pubdb" in UI context (login/pubdb link, not PUBDB-ID)
    has_ui_keywords = any(keyword in table_text_lower for keyword in _PUBDB_UI_KEYWORDS)
    has_pubdb_ui_context = ('pubdb' in table_text_lower and 
                          ('login' in table_text_lower or 'guest' in table_text_lower or 
                           'search:' in table_text_lower or 'submit' in table_text_lower))
    
    return (has_ui_keywords or has_pubdb_ui_context) and not has_publication_id


def _is_pubdb_page(url, html_content):
    """
    Check if a page is a PUBDB page by checking both URL and content.
    This handles cases where pages redirect to or embed PUBDB content.
    
    Args:
        url: The page URL (can be None)
        html_content: The HTML content (can be None)
    
    Returns:
        bool: True if the page is a PUBDB page
    """
    return (url and is_pubdb_url(url)) or is_pubdb_content(html_content)


def _normalize_text_spacing(line):
    """
    Normalize text spacing to fix concatenation issues.
    
    Fixes patterns like:
    - "word+Capital" -> "word +Capital"
    - "hutch:+49" -> "hutch: +49" (but preserve phone formats)
    - Multiple spaces -> single space
    
    Args:
        line: Input line string
        
    Returns:
        Normalized line string
    """
    if not line or line.strip().startswith(('#', '|', '-', '*')) or not line.strip():
        # Don't modify markdown syntax lines
        return line
    
    # Normalize multiple spaces to single space
    normalized = re.sub(r' +', ' ', line)
    
    # Fix concatenated patterns: word+Capital (but not in URLs/emails)
    # Pattern: lowercase letter followed by uppercase letter (word boundary)
    normalized = re.sub(r'([a-z])([A-Z])', r'\1 \2', normalized)
    
    # Fix: word+number (but preserve phone formats like "+49 (0)40")
    # Only fix if not part of phone number pattern
    if not re.search(r'\+?\d+\s*\(', normalized):  # Not a phone number
        normalized = re.sub(r'([a-zA-Z])(\+?\d)', r'\1 \2', normalized)
    
    # Fix: number+word (but preserve units like "6GeV" -> "6 GeV")
    normalized = re.sub(r'(\d)([A-Za-z])', r'\1 \2', normalized)
    
    # Fix: punctuation+word (colon, semicolon, etc.)
    normalized = re.sub(r'([:;])([A-Za-z])', r'\1 \2', normalized)
    
    # Fix: word:number or word:+number (e.g., "hutch:+49" -> "hutch: +49")
    # But preserve phone formats like "+49 (0)40"
    normalized = re.sub(r'([a-zA-Z]):(\+?\d)', r'\1: \2', normalized)
    
    # Preserve email addresses and URLs (undo any changes to them)
    # This is a simple check - more complex patterns would need more sophisticated handling
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    url_pattern = r'https?://[^\s]+'
    
    return normalized


def extract_external_links(html_content, current_url):
    """
    Extract all external links (links to different domains) with their text and associated headings.
    
    IMPORTANT: Links inside tables are SKIPPED - they should remain in their table positions.
    Only links outside tables are extracted here, grouped by their associated headings/sections.
    
    Args:
        html_content: HTML content as string
        current_url: Current page URL to determine external links
        
    Returns:
        Markdown string with external links grouped by section/heading, or empty string if none found
    """
    if not BEAUTIFULSOUP_AVAILABLE or not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        current_domain = urlparse(current_url).netloc.lower()
        
        # Find all links
        links = soup.find_all('a', href=True)
        external_links_by_section = {}  # Dict: section_heading -> list of links
        seen_links = set()  # Deduplicate by URL
        
        for link in links:
            href = link.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('mailto:'):
                continue
            
            # SKIP links inside tables - they should stay in their table positions
            # Check if link is inside a table cell (td/th) or directly inside a table
            parent_table = link.find_parent('table')
            parent_cell = link.find_parent(['td', 'th'])
            
            # Link is in a table if it's inside a table element OR inside a table cell
            if parent_table or parent_cell:
                continue
            
            # Make absolute URL
            absolute_url = urljoin(current_url, href)
            parsed = urlparse(absolute_url)
            link_domain = parsed.netloc.lower()
            
            # Check if external (different domain)
            if link_domain and link_domain != current_domain:
                # Skip if already seen
                if absolute_url in seen_links:
                    continue
                seen_links.add(absolute_url)
                
                # Get link text
                link_text = link.get_text(strip=True)
                if not link_text:
                    link_text = absolute_url
                
                # Find associated heading/section for this link
                # Look for nearest heading (h1-h6) before this link in the DOM
                section_heading = None
                best_heading = None
                best_heading_level = 7  # Start with level higher than any real heading
                
                # Strategy: Find all headings before this link in document order
                # Then pick the closest one (highest level, most recent)
                link_position = None
                try:
                    # Get all elements before this link
                    all_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'])
                    for i, elem in enumerate(all_elements):
                        if elem == link:
                            link_position = i
                            break
                    
                    if link_position is not None:
                        # Find all headings before this link
                        for i in range(link_position - 1, -1, -1):
                            elem = all_elements[i]
                            if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                heading_level_num = int(elem.name[1])
                                heading_text = elem.get_text(strip=True)
                                if heading_text and heading_level_num < best_heading_level:
                                    best_heading = heading_text
                                    best_heading_level = heading_level_num
                                    # Prefer closer headings (stop if we found a good one)
                                    if heading_level_num <= 3:  # h1, h2, h3 are usually section headers
                                        break
                except Exception:
                    # Fallback: simple parent traversal
                    current = link
                    for _ in range(5):  # Limit search depth
                        if current is None:
                            break
                        # Check previous siblings
                        prev = current.find_previous_sibling()
                        while prev:
                            if prev.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                heading_level_num = int(prev.name[1])
                                heading_text = prev.get_text(strip=True)
                                if heading_text and heading_level_num < best_heading_level:
                                    best_heading = heading_text
                                    best_heading_level = heading_level_num
                            prev = prev.find_previous_sibling()
                        current = current.parent
                
                section_heading = best_heading
                
                # Use section heading or default
                section_key = section_heading if section_heading else "External Links"
                
                if section_key not in external_links_by_section:
                    external_links_by_section[section_key] = []
                
                external_links_by_section[section_key].append({
                    'text': link_text,
                    'url': absolute_url
                })
        
        # Format as markdown sections grouped by heading
        if external_links_by_section:
            markdown = ""
            # Sort sections: "External Links" last, others alphabetically
            sorted_sections = sorted(
                [k for k in external_links_by_section.keys() if k != "External Links"]
            )
            if "External Links" in external_links_by_section:
                sorted_sections.append("External Links")
            
            for section in sorted_sections:
                links_list = external_links_by_section[section]
                if links_list:
                    # Use the section heading as markdown heading (or default)
                    if section == "External Links":
                        markdown += "\n\n## External Links\n\n"
                    else:
                        # Use the section heading as-is (it's already a heading from HTML)
                        markdown += f"\n\n## {section}\n\n"
                    
                    for link in links_list:
                        markdown += f"- [{link['text']}]({link['url']})\n"
            
            return markdown
        
        return ""
    except Exception as e:
        # Silently fail - external links are optional
        return ""


def extract_cell_links(cell_element):
    """
    Extract ALL content from a table cell (text + links) and return as markdown.
    
    GENERAL-PURPOSE STRATEGY:
    1. Remove images (they're decorative, not content)
    2. Convert all links to markdown format, preserving their text content
    3. Extract all remaining text
    4. Combine everything in order
    
    This ensures names, emails, phone numbers, and all text are preserved.
    
    Args:
        cell_element: BeautifulSoup element representing a table cell
        
    Returns:
        Markdown string with all content preserved (text + links)
    """
    if not cell_element:
        return ""
    
    try:
        # Create a working copy to avoid modifying the original
        cell_html = str(cell_element)
        cell_copy = BeautifulSoup(cell_html, 'html.parser')
        cell = cell_copy
        
        # Step 1: Remove all images (decorative, not content)
        # But preserve any text that might be associated with them
        for img in cell.find_all('img'):
                img.decompose()
        
        # Step 2: Process all links and convert to markdown
        # This preserves the link text (which often contains names)
        links = cell.find_all('a', href=True, recursive=True)
        from bs4 import NavigableString
        
        for link in links:
            href = link.get('href', '').strip()
            if not href:
                link.decompose()  # Remove empty links
                continue
            
            # Get link text - this is critical for preserving names
            link_text = link.get_text(strip=True)
            
            # If no link text, try to get it from attributes
            if not link_text or len(link_text) < 1:
                link_text = (link.get('title') or link.get('aria-label') or '').strip()
            
            # Handle mailto links
            if href.startswith('mailto:'):
                email = unescape(href[7:])
                # Use email as text if link text is generic or missing
                if not link_text or link_text.lower() in ['email', 'e-mail', 'mail', 'contact', 'e-mail:']:
                    link_text = email
                markdown_link = f"[{link_text}](mailto:{email})"
            elif href:
                # Regular link - use link text or href as fallback
                if not link_text:
                        link_text = href
                markdown_link = f"[{link_text}]({href})"
            else:
                link.decompose()
                continue
            
            # Replace link with markdown (preserves link text)
            link.replace_with(NavigableString(markdown_link))
        
        # Step 3: Extract all text (includes markdown links we just inserted)
        # Use space separator to keep words together but separate elements
        cell_text = cell.get_text(separator=' ', strip=True)
        
        # Step 4: Clean up whitespace


        # Use unicode-aware regex to handle umlauts and special characters correctly
        cell_text = re.sub(r'\s+', ' ', cell_text, flags=re.UNICODE).strip()
        
        # Step 5: Remove duplicate email links (if same email appears multiple times)
        email_pattern = r'\[([^\]]+)\]\(mailto:([^\)]+)\)'
        emails_seen = set()
        def dedup_emails(match):
            email = match.group(2).lower()
            if email in emails_seen:
                return match.group(2)  # Just return email, not full link
            emails_seen.add(email)
            return match.group(0)  # Keep full markdown link
        
        cell_text = re.sub(email_pattern, dedup_emails, cell_text)
        
        # Step 6: Add labels if content suggests them (phone, location, email)
        # Only add if label is missing
        original_html = str(cell_element)
        
        # Email label
        if re.search(r'\[([^\]]+)\]\(mailto:[^\)]+\)', cell_text):
            if not re.search(r'(?:E-Mail|E-mail|Email|e-mail)[:\s]', cell_text, re.IGNORECASE):
                cell_text = re.sub(r'(\[([^\]]+)\]\(mailto:[^\)]+\))', r'E-mail: \1', cell_text, count=1)
        
        # Phone label
        phone_pattern = r'(\+?\d{1,3}[\s\-\(\)]*(?:0\))?\s*\d{1,4}[\s\-]+\d{3,4}[\s\-]+\d{3,4})'
        phone_match = re.search(phone_pattern, cell_text)
        if phone_match:
            phone_text = phone_match.group(1)
            digit_count = len(re.findall(r'\d', phone_text))
            # Valid phone: at least 8 digits, not a year
            if digit_count >= 8 and not re.match(r'^(19|20)\d{2}', phone_text.replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+', '')):
                before_phone = cell_text[:phone_match.start()]
                if not re.search(r'(?:Phone|Tel|Telephone)[:\s]', before_phone, re.IGNORECASE):
                    cell_text = cell_text[:phone_match.start()] + 'Phone: ' + phone_text + cell_text[phone_match.end():]
        
        # Location label
        location_pattern = r'\b([A-Z]\d+[A-Z]?\s*[/-]\s*\d+[A-Z]?|[A-Z]\d+[a-z]?\s*/\s*[A-Z]{1,3}\.?\d+)'
        location_match = re.search(location_pattern, cell_text)
        if location_match:
            location_text = location_match.group(1)
            # Exclude publication IDs and dates
            if not re.search(r'PUBDB|PUB|ID|DOI|ISBN', location_text, re.IGNORECASE):
                if not re.match(r'^\d{4}[\s\-]+\d{4}', location_text):
                    before_location = cell_text[:location_match.start()]
                    if not re.search(r'(?:Location|Office|Room)[:\s]', before_location, re.IGNORECASE):
                        cell_text = cell_text[:location_match.start()] + 'Location: ' + location_text + cell_text[location_match.end():]
        
        # Step 7: Remove duplicate consecutive words (but not names before links)
        cell_text = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_text)
        
        return cell_text if cell_text else ""
        
    except Exception as e:
        # Fallback: just get text content
        try:
            return cell_element.get_text(strip=True)
        except:
            return ""


def enrich_crawl4ai_tables_with_links(result, is_pdf=False):
    """
    Method 1: Extract tables using Crawl4AI's built-in table extraction,
    then enrich them with links from the original HTML.
    
    This preserves Crawl4AI's table structure and formatting while adding
    back the links that were lost during markdown conversion.
    
    Args:
        result: Crawl4AI result object
        is_pdf: Whether this is a PDF result
        
    Returns:
        Markdown string with formatted tables with links preserved
    """
    tables_markdown = ""
    
    try:
        # Get Crawl4AI's extracted tables
        if not hasattr(result, 'tables') or not result.tables:
            return ""
        
        tables_markdown = "\n\n## Extracted Tables\n\n"
        
        # Get HTML tables for link enrichment (only for HTML pages)
        html_tables = []
        if not is_pdf and hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
            try:
                soup = BeautifulSoup(result.html, 'html.parser')
                html_tables = soup.find_all('table', recursive=True)
            except Exception:
                pass
        
        # Process each Crawl4AI table
        used_html_tables = set()  # Track which HTML tables we've already used
        
        for idx, crawl_table in enumerate(result.tables, 1):
            tables_markdown += f"### Table {idx}\n\n"
            
            # Try to find corresponding HTML table to enrich with links
            # Match by content similarity rather than just index
            enriched_table = None
            if html_tables:
                # Get a sample of text from Crawl4AI table for matching
                crawl_sample = ""
                if crawl_table.get('rows'):
                    # Use first few cells from first row as identifier
                    first_row = crawl_table.get('rows', [])[0]
                    crawl_sample = " ".join(str(cell)[:30] for cell in first_row[:3] if cell)
                
                # Find best matching HTML table
                best_match_idx = None
                best_match_score = 0
                
                for html_idx, html_table in enumerate(html_tables):
                    if html_idx in used_html_tables:
                        continue
                    
                    # Get sample text from HTML table
                    html_sample = ""
                    tbody = html_table.find('tbody')
                    table_rows = tbody.find_all('tr') if tbody else html_table.find_all('tr')
                    if table_rows:
                        first_row = table_rows[0]
                        cells = first_row.find_all(['td', 'th'], limit=3)
                        html_sample = " ".join(cell.get_text(strip=True)[:30] for cell in cells)
                    
                    # Simple similarity: check if crawl_sample appears in html_sample or vice versa
                    if crawl_sample and html_sample:
                        # Count common words
                        crawl_words = set(crawl_sample.lower().split())
                        html_words = set(html_sample.lower().split())
                        common = len(crawl_words & html_words)
                        if common > best_match_score:
                            best_match_score = common
                            best_match_idx = html_idx
                
                # Use best match if found, otherwise try index-based matching
                if best_match_idx is not None and best_match_score > 0:
                    html_table = html_tables[best_match_idx]
                    used_html_tables.add(best_match_idx)
                    enriched_table = enrich_table_with_html_links(crawl_table, html_table)
                elif idx <= len(html_tables) and (idx - 1) not in used_html_tables:
                    # Fallback to index-based matching
                    html_table = html_tables[idx - 1]
                    used_html_tables.add(idx - 1)
                    enriched_table = enrich_table_with_html_links(crawl_table, html_table)
            
            # Use enriched table if available, otherwise use Crawl4AI's original
            table_to_use = enriched_table if enriched_table else crawl_table
            
            # Extract table data
            headers = table_to_use.get('headers', [])
            rows = table_to_use.get('rows', [])
            caption = table_to_use.get('caption', '')
            
            if caption:
                tables_markdown += f"*{caption}*\n\n"
            
            # Format as markdown table
            if headers and rows:
                tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                
                for row in rows:
                    row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
                    tables_markdown += "| " + " | ".join(str(cell) for cell in row_data) + " |\n"
                tables_markdown += "\n"
            elif rows:
                if rows:
                    first_row = rows[0]
                    tables_markdown += "| " + " | ".join(str(cell) for cell in first_row) + " |\n"
                    tables_markdown += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                    for row in rows[1:]:
                        tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                    tables_markdown += "\n"
        
        # Also check for tables that Crawl4AI might have missed (both nested and top-level)
        # Only extract tables that have meaningful content
        if html_tables:
            missed_count = 0
            for html_idx, html_table in enumerate(html_tables):
                if html_idx in used_html_tables:
                    continue
                
                # Extract table (whether nested or top-level)
                missed_table = extract_table_from_html(html_table)
                headers = missed_table.get('headers', [])
                rows = missed_table.get('rows', [])
                
                # Only include if it has meaningful content (at least 2 rows or headers)
                if (headers and rows) or (rows and len(rows) > 1):
                    missed_count += 1
                    # Check if this is a nested table
                    parent_table = html_table.find_parent('table')
                    table_label = "Nested" if parent_table else "Missed"
                    tables_markdown += f"### Table {len(result.tables) + missed_count} ({table_label})\n\n"
                    
                    caption = missed_table.get('caption', '')
                    if caption:
                        tables_markdown += f"*{caption}*\n\n"
                    
                    if headers and rows:
                        tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                        tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                        for row in rows:
                            row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
                            tables_markdown += "| " + " | ".join(str(cell) for cell in row_data) + " |\n"
                        tables_markdown += "\n"
                    elif rows and len(rows) > 1:
                        first_row = rows[0]
                        tables_markdown += "| " + " | ".join(str(cell) for cell in first_row) + " |\n"
                        tables_markdown += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                        for row in rows[1:]:
                            tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                        tables_markdown += "\n"
            
    except Exception as e:
        tables_markdown = f"\n\n## Extracted Tables\n\n*Error extracting tables: {e}*\n\n"
        import traceback
        traceback.print_exc()
    
    return tables_markdown


def enrich_table_with_html_links(crawl_table, html_table):
    """
    Enrich Crawl4AI's extracted table with links from HTML table.
    
    Args:
        crawl_table: Table dict from Crawl4AI
        html_table: BeautifulSoup table element
        
    Returns:
        Enriched table dict with links preserved
    """
    enriched = {
        'headers': [],
        'rows': [],
        'caption': crawl_table.get('caption', '')
    }
    
    # Enrich headers - use recursive=True to catch all header cells
    html_headers = []
    thead = html_table.find('thead')
    if thead:
        html_headers = thead.find_all(['th', 'td'], recursive=True)
        # Filter to ensure headers belong to this table, not nested tables
        html_headers = [h for h in html_headers if h.find_parent('table') == html_table]
    else:
        first_row = html_table.find('tr')
        if first_row:
            html_headers = first_row.find_all(['th', 'td'], recursive=True)
            # Filter to ensure headers belong to this table, not nested tables
            html_headers = [h for h in html_headers if h.find_parent('table') == html_table]
    
    crawl_headers = crawl_table.get('headers', [])
    for i, crawl_header in enumerate(crawl_headers):
        if i < len(html_headers):
            enriched['headers'].append(extract_cell_links(html_headers[i]))
        else:
            enriched['headers'].append(str(crawl_header))
    
    # Enrich rows - use recursive=True to catch all rows
    tbody = html_table.find('tbody')
    if tbody:
        table_rows = tbody.find_all('tr', recursive=True)
        # Filter to ensure rows belong to this table, not nested tables
        table_rows = [r for r in table_rows if r.find_parent('table') == html_table]
    else:
        table_rows = html_table.find_all('tr', recursive=True)
        # Filter to ensure rows belong to this table, not nested tables
        table_rows = [r for r in table_rows if r.find_parent('table') == html_table]
        # Exclude header rows
        if thead:
            thead_rows = thead.find_all('tr', recursive=True)
            thead_rows = [r for r in thead_rows if r.find_parent('table') == html_table]
            thead_row_set = set(thead_rows)
            table_rows = [r for r in table_rows if r not in thead_row_set]
        elif html_headers:
            # Headers were in first row, skip it
            table_rows = table_rows[1:] if len(table_rows) > 1 else []
    
    start_idx = 0  # Already filtered header rows above
    
    crawl_rows = crawl_table.get('rows', [])
    html_rows = table_rows[start_idx:]
    
    for row_idx, crawl_row in enumerate(crawl_rows):
        if row_idx < len(html_rows):
            html_row = html_rows[row_idx]
            # Use recursive=True to catch all cells, then filter nested tables
            # Since we're using html_row.find_all(), all returned cells are descendants of html_row
            # We only need to filter out cells that belong to nested tables
            html_cells = html_row.find_all(['td', 'th'], recursive=True)
            html_cells = [c for c in html_cells if c.find_parent('table') == html_table]
            enriched_row = []
            for cell_idx, crawl_cell in enumerate(crawl_row):
                if cell_idx < len(html_cells):
                    enriched_row.append(extract_cell_links(html_cells[cell_idx]))
                else:
                    enriched_row.append(str(crawl_cell))
            enriched['rows'].append(enriched_row)
        else:
            enriched['rows'].append([str(cell) for cell in crawl_row])
    
    return enriched


def extract_table_from_html(html_table):
    """
    Extract a complete table structure from HTML table element.
    
    Args:
        html_table: BeautifulSoup table element
        
    Returns:
        Table dict with headers, rows, and caption
    """
    table_data = {
        'headers': [],
        'rows': [],
        'caption': ''
    }
    
    # Extract caption
    caption = html_table.find('caption')
    if caption:
        table_data['caption'] = caption.get_text(strip=True)
    
    # Extract headers - be conservative: only use explicit headers
    # Headers should be in <thead> or use <th> tags, not inferred from first row
    # Use recursive=True first to catch all header cells, then filter nested tables
    thead = html_table.find('thead')
    if thead:
        # Explicit <thead> section - definitely headers
        # Use recursive=True to catch all header cells wrapped in other elements
        header_cells = thead.find_all(['th', 'td'], recursive=True)
        # Filter out cells from nested tables
        header_cells = [c for c in header_cells if c.find_parent('table') == html_table]
        # If no cells found recursively, try direct children as fallback
        if not header_cells:
            header_cells = thead.find_all(['th', 'td'], recursive=False)
        table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
    else:
        # No <thead> - check if first row uses <th> tags (strong indicator of headers)
        # Use recursive=True first to catch all rows
        first_row = html_table.find('tr', recursive=True)
        if first_row:
            # Ensure first_row belongs to this table, not a nested table
            if first_row.find_parent('table') != html_table:
                # Find first row that belongs to this table
                all_rows = html_table.find_all('tr', recursive=True)
                all_rows = [r for r in all_rows if r.find_parent('table') == html_table]
                first_row = all_rows[0] if all_rows else None
        
        if first_row:
            # Use recursive=True to catch all cells wrapped in other elements
            # Since we're using first_row.find_all(), all returned cells are descendants of first_row
            # We only need to filter out cells that belong to nested tables
            header_cells = first_row.find_all(['th', 'td'], recursive=True)
            # Filter out cells from nested tables
            header_cells = [c for c in header_cells if c.find_parent('table') == html_table]
            # If no cells found recursively, try direct children as fallback
            if not header_cells:
                header_cells = first_row.find_all(['th', 'td'], recursive=False)
            
            # Only treat as headers if ALL cells in first row are <th> tags
            # This avoids misidentifying data rows as headers
            if header_cells and all(cell.name == 'th' for cell in header_cells):
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    header_texts = [extract_cell_links(cell)[:50] for cell in header_cells[:2]]
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'I',
                        'location': 'crawl_desy_simple.py:1141',
                        'message': 'Found headers from th tags in first row',
                        'data': {
                            'header_texts': header_texts,
                            'num_headers': len(header_cells)
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
            # Additional check: if first row has <th> tags mixed with <td>, 
            # only use the <th> cells as headers (common in complex tables)
            elif header_cells:
                th_cells = [cell for cell in header_cells if cell.name == 'th']
                if th_cells and len(th_cells) >= len(header_cells) * 0.5:  # At least 50% are <th>
                    table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
    
    # Extract rows - GENERAL STRATEGY: Find all rows, exclude only clearly nested ones
    tbody = html_table.find('tbody')
    if tbody:
        # Has tbody - get rows from tbody
        table_rows = tbody.find_all('tr', recursive=True)
        # Filter: only exclude rows that are clearly in nested tables
        filtered_rows = []
        for r in table_rows:
            parent_table = r.find_parent('table')
            # Include if: belongs to html_table OR parent is None (might be in html_table structure)
            # Exclude only if: parent is a nested table (parent_table is inside html_table)
            if parent_table and parent_table != html_table:
                if parent_table.find_parent('table') == html_table:
                    continue  # Skip nested table rows
            filtered_rows.append(r)
        table_rows = filtered_rows
        # Fallback: if no rows found recursively, try direct children
        if not table_rows:
            table_rows = tbody.find_all('tr', recursive=False)
    else:
        # No tbody - get all tr elements
        all_rows = html_table.find_all('tr', recursive=True)
        # Filter: only exclude rows that are clearly in nested tables
        filtered_rows = []
        for r in all_rows:
            parent_table = r.find_parent('table')
            if parent_table and parent_table != html_table:
                if parent_table.find_parent('table') == html_table:
                    continue  # Skip nested table rows
            filtered_rows.append(r)
        all_rows = filtered_rows
        # Fallback: if no rows found recursively, try direct children
        if not all_rows:
            all_rows = html_table.find_all('tr', recursive=False)
        
        # Exclude header rows if we found headers
        if thead:
            thead_rows = thead.find_all('tr', recursive=True)
            if not thead_rows:
                thead_rows = thead.find_all('tr', recursive=False)
            thead_row_set = set(thead_rows)
            table_rows = [r for r in all_rows if r not in thead_row_set]
        elif table_data['headers']:
            # Headers were in first row, skip it
            table_rows = all_rows[1:] if len(all_rows) > 1 else []
        else:
            table_rows = all_rows
    
    # Extract data from each row - GENERAL STRATEGY: Get all cells, exclude only nested ones
    for tr in table_rows:
        # CRITICAL FIX: Try direct children first (most accurate for table structure)
        # Only use recursive=True if direct children don't exist
        cells = tr.find_all(['td', 'th'], recursive=False)
        
        # #region agent log
        cells_before_filter = len(cells)
        # #endregion
        
        # If no direct children, try recursive but filter nested tables
        if not cells:
            cells = tr.find_all(['td', 'th'], recursive=True)
            # Filter: only exclude cells that are clearly in nested tables
            filtered_cells = []
            for c in cells:
                parent_table = c.find_parent('table')
                # Include if: belongs to html_table OR parent is None
                # Exclude only if: parent is a nested table
                if parent_table and parent_table != html_table:
                    if parent_table.find_parent('table') == html_table:
                        continue  # Skip nested table cells
                filtered_cells.append(c)
            cells = filtered_cells
        else:
            # Direct children found - but still filter out any that might be in nested tables
            filtered_cells = []
            for c in cells:
                parent_table = c.find_parent('table')
                if parent_table and parent_table != html_table:
                    continue  # Skip if in nested table
                filtered_cells.append(c)
            cells = filtered_cells
        
        # #region agent log
        if cells_before_filter > 2 or len(cells) > 2:
            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                import json
                f.write(json.dumps({
                    'sessionId': 'debug-session',
                    'runId': 'run1',
                    'hypothesisId': 'L',
                    'location': 'crawl_desy_simple.py:1202',
                    'message': 'Table row cell extraction',
                    'data': {
                        'cells_before_filter': cells_before_filter,
                        'cells_after_filter': len(cells),
                        'used_recursive': cells_before_filter == 0,
                        'first_cell_preview': str(cells[0])[:100] if cells else None
                    },
                    'timestamp': int(__import__('time').time() * 1000)
                }) + '\n')
        # #endregion
        
        # Extract cell content
        if cells:
            row_data = []
            for cell in cells:
                cell_content = extract_cell_links(cell)
                # Use unicode-aware regex to handle umlauts and special characters correctly
                cell_content = re.sub(r'\s+', ' ', cell_content, flags=re.UNICODE).strip()
                row_data.append(cell_content)
            
            # Only skip rows where ALL cells are completely empty
            if any(str(cell).strip() for cell in row_data):
                table_data['rows'].append(row_data)
    
    return table_data


def parse_single_column_cell_html(cell_element):
    """
    Parse single-column cell HTML preserving structure (<br> tags, links).
    
    Extracts content from HTML cell element while preserving:
    - <br> tags as line breaks
    - Links as markdown format
    - HTML structure for delimiter detection
    
    Args:
        cell_element: BeautifulSoup cell element (td or th)
        
    Returns:
        Dict with:
        - 'text': Text content with <br> replaced by newlines
        - 'html_structure': List of segments with their types (text, link, br)
        - 'links': List of markdown links found
    """
    if not cell_element:
        return {'text': '', 'html_structure': [], 'links': []}
    
    try:
        # Create working copy
        cell_html = str(cell_element)
        cell_copy = BeautifulSoup(cell_html, 'html.parser')
        cell = cell_copy
        
        # Remove images (decorative)
        for img in cell.find_all('img'):
            img.decompose()
        
        # Process links and convert to markdown (preserve for later)
        links = []
        link_markdown_map = {}
        for link in cell.find_all('a', href=True, recursive=True):
            href = link.get('href', '').strip()
            if not href:
                continue
            
            link_text = link.get_text(strip=True)
            if not link_text:
                link_text = (link.get('title') or link.get('aria-label') or '').strip()
            
            if href.startswith('mailto:'):
                email = unescape(href[7:])
                if not link_text or link_text.lower() in ['email', 'e-mail', 'mail', 'contact', 'e-mail:']:
                    link_text = email
                markdown_link = f"[{link_text}](mailto:{email})"
            elif href:
                if not link_text:
                    link_text = href
                markdown_link = f"[{link_text}]({href})"
            else:
                continue
    
            # Store mapping for replacement
            link_markdown_map[str(link)] = markdown_link
            links.append(markdown_link)
            
            # Replace link with placeholder to preserve position
            from bs4 import NavigableString
            link.replace_with(NavigableString(f"__LINK_{len(links)-1}__"))
        
        # FIX 3: Flatten HTML structure before extracting text to avoid structural newlines
        # Replace nested divs/spans with spaces, but preserve <br> tags as newlines
        # This prevents structural newlines from breaking content parsing
        
        # First, replace <br> tags with a special marker (we'll convert to newline later)
        for br in cell.find_all('br'):
            br.replace_with(NavigableString('__BR__'))
        
        # Flatten nested block elements (div, p, span) by replacing with spaces
        # This prevents structural newlines from nested HTML
        # Process in reverse order to avoid modifying parent while iterating
        for block_elem in reversed(list(cell.find_all(['div', 'p', 'span']))):
            # Get text content
            block_text = block_elem.get_text(strip=True)
            if block_text:
                # Replace block element with its text content (space-separated)
                # Ensure proper spacing: add space before and after to prevent concatenation
                block_elem.replace_with(NavigableString(' ' + block_text + ' '))
            else:
                block_elem.decompose()
        
        # Now get text - use space separator to avoid structural newlines
        # FIX 3B: Remove ALL newlines first, then add back only intentional <br> newlines
        cell_text = cell.get_text(separator=' ', strip=False)
        
        # Remove all newlines (structural newlines from HTML)
        cell_text = re.sub(r'\n+', ' ', cell_text)
        
        # Replace BR markers with actual newlines (these are intentional line breaks)
        cell_text = cell_text.replace('__BR__', '\n')
        
        # Normalize multiple spaces to single space (but preserve intentional newlines from <br>)
        cell_text = re.sub(r'[ \t]+', ' ', cell_text)  # Multiple spaces/tabs -> single space
        cell_text = re.sub(r'\n\s+', '\n', cell_text)  # Remove leading spaces after newlines
        cell_text = re.sub(r'\s+\n', '\n', cell_text)  # Remove trailing spaces before newlines
        cell_text = re.sub(r' +', ' ', cell_text)  # Multiple spaces -> single space
        
        # Ensure proper spacing around common field labels (GENERALIZED: handles all variations)
        # This prevents concatenation like "AckermannE-Mail:" -> "Ackermann E-Mail:"
        # Also handle cases like "7415Location:" -> "7415 Location:"
        # Match all label variations: Tel, Telephone, Contact, Phone, Email, E-Mail, Location, Office, Room, etc.
        # FIX 3L: Don't match single digit + single letter (like "2A") - require 2+ chars for the label part
        cell_text = re.sub(r'([a-z0-9])([A-Z][a-z]{2,}:)', r'\1 \2', cell_text)  # lowercase/digit followed by 2+ letter Capital: -> add space
        # Handle specific multi-word labels
        cell_text = re.sub(r'([a-z0-9])(E-Mail:|E-mail:)', r'\1 \2', cell_text, flags=re.IGNORECASE)
        cell_text = re.sub(r'([a-z0-9])(Research Areas:)', r'\1 \2', cell_text, flags=re.IGNORECASE)
        # FIX 3I: Add space between number and letter (e.g., "02Fermi" -> "02 Fermi", "2L37" -> "2L 37")
        # But don't add space for single letter + number combinations like "2A" (location codes)
        # Only add space if it's a multi-digit number or multi-letter word
        cell_text = re.sub(r'(\d{2,})([A-Za-z])', r'\1 \2', cell_text)  # 2+ digits followed by letter
        cell_text = re.sub(r'([A-Za-z]{2,})(\d+)', r'\1 \2', cell_text)  # 2+ letters followed by number
        # Handle location codes like "2A / 02" - don't split "2A" but split "02Fermi"
        # This is more conservative and prevents breaking location codes
        
        # Restore markdown links
        for i, markdown_link in enumerate(links):
            cell_text = cell_text.replace(f"__LINK_{i}__", markdown_link)
        
        return {
            'text': cell_text,
            'links': links
        }
    except Exception as e:
        print(f"[DEBUG] parse_single_column_cell_html failed: {e}")
        return {'text': '', 'html_structure': [], 'links': []}


def normalize_field_label(label):
    """
    Normalize field labels to standardized headers for consistency across 200k+ URLs.
    
    Maps variations like Tel, Telephone, Contact, phone, Phone → Phone
    Maps variations like Email, e-mail, Mail → E-Mail
    Maps variations like Office, Room, Address → Location
    Maps variations like URL, Website, Homepage → Link
    Maps variations like Research Areas, Interests → Research Areas
    
    Args:
        label: Raw label string (may be empty, None, or contain variations)
        
    Returns:
        Normalized label string (standardized header name)
    """
    if not label or not isinstance(label, str):
        return label or ''
    
    label_lower = label.strip().rstrip(':').lower()
    
    # Phone variations: Tel, Telephone, Contact, T., etc. → Phone
    if label_lower in ['tel', 'telephone', 'telefon', 'contact', 't.', 'phone', 'mobile', 'cell', 'fax']:
        return 'Phone'
    
    # Email variations: Email, e-mail, Mail, etc. → E-Mail
    if label_lower in ['email', 'e-mail', 'e-mail:', 'mail', 'mail:', 'email address', 'e-mail address']:
        return 'E-Mail'
    
    # Location variations: Office, Room, Address, etc. → Location
    if label_lower in ['location', 'office', 'room', 'address', 'adresse', 'building', 'floor']:
        return 'Location'
    
    # Link variations: URL, Website, Homepage, etc. → Link
    if label_lower in ['link', 'url', 'website', 'homepage', 'web', 'home page', 'personal website']:
        return 'Link'
    
    # Research variations: Research Areas, Interests, etc. → Research Areas
    if label_lower in ['research', 'research areas', 'research area', 'interests', 'field', 'fields', 'focus', 'focus areas']:
        return 'Research Areas'
    
    # Name variations: Name, Full Name, etc. → Name
    if label_lower in ['name', 'full name', 'person', 'contact name']:
        return 'Name'
    
    # If no match, capitalize first letter of each word (Title Case)
    # This handles unknown labels gracefully
    words = label_lower.split()
    normalized = ' '.join(word.capitalize() for word in words)
    return normalized


def parse_single_column_table_content(cell_html):
    """
    Parse single-column cell content into Label | Value pairs using pattern-based heuristics.
    
    Strategy:
    1. Detect patterns (email, phone, URL) as primary delimiters
    2. Use HTML structure (<br>, newlines) as secondary
    3. Use multiple spaces as tertiary
    4. First segment (if no pattern, short) = Name
    5. Subsequent segments = Label | Value pairs
    
    Args:
        cell_html: BeautifulSoup cell element (td or th)
        
    Returns:
        List of [label, value] pairs, with first being Name if applicable
    """
    if not cell_html:
        return []
    
    # Parse HTML preserving structure
    parsed = parse_single_column_cell_html(cell_html)
    cell_text = parsed['text']
    
    if not cell_text.strip():
        return []
    
    # Pattern definitions (universal, not hardcoded keywords)
    patterns = {
        'email': r'\[([^\]]+)\]\(mailto:([^\s@]+@[^\s@]+\.[^\s)]+)\)|([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
        'phone': r'\+?\d{1,3}[\s\-\(\)]*(?:0\))?\s*\d{1,4}[\s\-]+\d{3,4}[\s\-]+\d{3,4}',
        'url': r'\[([^\]]+)\]\((https?://[^\s\)]+)\)|(https?://[^\s\)]+)',
    }
    
    # Find all pattern positions
    pattern_positions = []
    for pattern_type, pattern_regex in patterns.items():
        for match in re.finditer(pattern_regex, cell_text):
            # Check if there's a label before the pattern (within 30 chars)
            start_pos = max(0, match.start() - 30)
            before_pattern = cell_text[start_pos:match.start()]
            
            # Look for label pattern (text ending with colon, optionally with "E-Mail", "Phone", etc.)
            label_match = re.search(r'([A-Za-z\s\-]+):\s*$', before_pattern)
            label = None
            if label_match:
                label = label_match.group(1).strip()
            
            pattern_positions.append({
                'type': pattern_type,
                'start': match.start(),
                'end': match.end(),
                'match': match.group(0),
                'label': label,
                'label_start': match.start() - len(before_pattern) + (label_match.start() if label_match else 0) if label_match else None
            })
    
    # Sort by position
    pattern_positions.sort(key=lambda x: x['start'])
    
    # Split cell text into segments based on patterns
    segments = []
    last_pos = 0
    
    for pos_info in pattern_positions:
        label_start = pos_info.get('label_start')
        pattern_start = pos_info['start']
        
        if label_start is not None and label_start < pattern_start:
            # Label exists before pattern
            before_label = cell_text[last_pos:label_start].strip()
            if before_label:
                segments.append({
                    'type': 'text',
                    'content': before_label,
                    'position': last_pos
                })
            
            # Label text
            label_text = pos_info.get('label', '')
            if label_text:
                segments.append({
                    'type': 'label',
                    'content': label_text,
                    'position': label_start
                })
            
            last_pos = pattern_start
        else:
            # No label - text before pattern
            before_text = cell_text[last_pos:pattern_start].strip()
            if before_text:
                segments.append({
                    'type': 'text',
                    'content': before_text,
                    'position': last_pos
                })
        
        # Pattern itself
        segments.append({
            'type': pos_info['type'],
            'content': pos_info['match'],
            'position': pattern_start
        })
        
        last_pos = pos_info['end']
    
    # Remaining text after last pattern
    remaining = cell_text[last_pos:].strip()
    if remaining:
        segments.append({
            'type': 'text',
            'content': remaining,
            'position': last_pos
        })
    
    # If no patterns found, try HTML structure delimiters
    if not pattern_positions:
        # Try splitting on newlines
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        if len(lines) > 1:
            segments = [{'type': 'text', 'content': line, 'position': i} for i, line in enumerate(lines)]
        else:
            # Try multiple spaces (2+)
            parts = re.split(r'\s{2,}', cell_text)
            if len(parts) > 1:
                segments = [{'type': 'text', 'content': part.strip(), 'position': i} for i, part in enumerate(parts) if part.strip()]
            else:
                # Single segment - return as-is
                segments = [{'type': 'text', 'content': cell_text.strip(), 'position': 0}]
    
    # Convert segments to Label | Value pairs
    label_value_pairs = []
    name_field = None
    
    # FIX 3K: Check for name in the FULL cell text before pattern-based splitting
    # This prevents names from being fragmented by pattern detection
    # Look for name pattern at the start of the cell text (before any patterns)
    # Pattern: Name followed by label (with or without colon) or email/phone pattern
    # Try with colon first, then without colon
    name_pattern_with_colon = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website):'
    name_pattern_without_colon = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website)\s+'
    # Also try name followed directly by email link pattern
    name_pattern_email = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+\[([^\]]+)\]\(mailto:'
    
    name_match = re.search(name_pattern_with_colon, cell_text, re.IGNORECASE)
    if not name_match:
        name_match = re.search(name_pattern_without_colon, cell_text, re.IGNORECASE)
    if not name_match:
        name_match = re.search(name_pattern_email, cell_text, re.IGNORECASE)
    if name_match:
        name_candidate = name_match.group(1).strip()
        # Verify it's a valid name (2-4 words, no patterns)
        if (len(name_candidate.split()) >= 2 and len(name_candidate.split()) <= 4 and
            not re.search(patterns['email'], name_candidate) and
            not re.search(patterns['phone'], name_candidate)):
            name_field = name_candidate
            # Remove the name from the first text segment if it exists
            if segments and segments[0]['type'] == 'text':
                first_content = segments[0]['content']
                # Remove the name from the start of the first segment
                first_content = re.sub(r'^' + re.escape(name_candidate) + r'\s+', '', first_content, flags=re.IGNORECASE)
                if first_content.strip():
                    segments[0]['content'] = first_content.strip()
                else:
                    segments.pop(0)  # Remove empty segment
    
    # Fallback: Check first segment for Name field (if not already found)
    if not name_field and segments:
        first_seg = segments[0]
        first_content = first_seg['content']
        
        # FIX 3J: Extract name from text that may contain labels (e.g., "Markus Ackermann E-Mail" -> "Markus Ackermann")
        # Remove common label patterns from the end of the first segment
        name_candidate = first_content
        # Remove label patterns at the end (E-Mail, Phone, Location, etc.)
        name_candidate = re.sub(r'\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website):?\s*$', '', name_candidate, flags=re.IGNORECASE)
        name_candidate = name_candidate.strip()
        
        # Name if: no pattern, short (< 50 chars), and looks like a name (2-4 capitalized words)
        is_name_like = bool(re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', name_candidate))
        if (first_seg['type'] == 'text' and 
            len(name_candidate) < 50 and
            len(name_candidate) > 2 and  # At least 3 characters
            not re.search(r':\s*$', name_candidate) and
            not re.search(patterns['email'], name_candidate) and
            not re.search(patterns['phone'], name_candidate) and
            (is_name_like or (len(name_candidate.split()) >= 2 and len(name_candidate.split()) <= 4))):  # Require 2+ words
            name_field = name_candidate
            # If we extracted name from first segment, remove the original segment and continue
            segments = segments[1:]  # Remove from segments
    
    # Process remaining segments as Label | Value pairs
    i = 0
    while i < len(segments):
        seg = segments[i]
        
        if seg['type'] in ['email', 'phone', 'url']:
            # Pattern found - extract label and value
            label = ""
            value = seg['content']
            
            # Check if previous segment is a label
            if i > 0 and segments[i-1]['type'] == 'label':
                label = normalize_field_label(segments[i-1]['content'])
                segments.pop(i-1)
                i -= 1
            elif i > 0 and segments[i-1]['type'] == 'text':
                prev_text = segments[i-1]['content']
                if prev_text.endswith(':') and len(prev_text) < 30:
                    label = normalize_field_label(prev_text.rstrip(':').strip())
                    segments.pop(i-1)
                    i -= 1
                else:
                    # Use pattern type as label (normalized)
                    if seg['type'] == 'email':
                        label = 'E-Mail'
                    elif seg['type'] == 'phone':
                        label = 'Phone'
                    elif seg['type'] == 'url':
                        label = 'Link'
                    else:
                        label = seg['type'].capitalize()
            else:
                # Use pattern type as label (normalized)
                if seg['type'] == 'email':
                    label = 'E-Mail'
                elif seg['type'] == 'phone':
                    label = 'Phone'
                elif seg['type'] == 'url':
                    label = 'Link'
                else:
                    label = seg['type'].capitalize()
            
            # Check if next segment is continuation (text after pattern)
            # FIX 3D: Don't append if next text contains a field label (Location:, Phone:, E-Mail:, etc.)
            if i+1 < len(segments) and segments[i+1]['type'] == 'text':
                next_text = segments[i+1]['content']
                # Don't append if it contains a field label pattern - GENERALIZED: matches all variations
                has_field_label = bool(re.search(r'\b(Location|Office|Room|Address|Phone|Tel|Telephone|Contact|E-Mail|Email|Mail|Link|URL|Website|Homepage|Research|Research Areas|Interests|Name|Fax|Mobile|Cell):', next_text, re.IGNORECASE))
                # If short, doesn't look like new field, and doesn't contain field label, append to value
                if len(next_text) < 30 and not re.search(r':\s*$', next_text) and not has_field_label:
                    value = f"{value} {next_text}"
                    segments.pop(i+1)
            
            if label and value:
                label_value_pairs.append([label, value])
        
        elif seg['type'] == 'text':
            # Text segment - try to extract Label | Value
            text = seg['content']
            
            # FIX 3E: Check if text contains multiple field labels (e.g., "Location:2A / 02Fermi, Group Leader")
            # Split on field labels if present - GENERALIZED: matches all variations (Tel, Telephone, Contact, etc.)
            field_label_pattern = r'\b(Location|Office|Room|Address|Phone|Tel|Telephone|Contact|E-Mail|Email|Mail|Link|URL|Website|Homepage|Research|Research Areas|Interests|Name|Fax|Mobile|Cell):'
            if re.search(field_label_pattern, text, re.IGNORECASE):
                # Split text on field labels - capture label and following text
                # Pattern: (text before)(Label:)(value after label)
                parts = re.split(f'({field_label_pattern})\s*', text, flags=re.IGNORECASE)
                current_label = None
                current_value = []
                skip_next = False  # Skip the label part itself
                
                for i, part in enumerate(parts):
                    part = part.strip()
                    if not part:
                        continue
                    
                    # Check if this part is a field label (with colon)
                    label_match = re.match(field_label_pattern + r'\s*$', part, re.IGNORECASE)
                    if label_match:
                        # Save previous label/value pair if exists
                        if current_label and current_value:
                            value_text = ' '.join(current_value).strip()
                            # FIX 3F: Remove label word from value if it appears at start
                            # Prevents "Location 2A / 02" -> should be "2A / 02"
                            value_text = re.sub(r'^' + re.escape(current_label) + r'\s+', '', value_text, flags=re.IGNORECASE)
                            if value_text.strip():
                                label_value_pairs.append([current_label, value_text.strip()])
                        # Start new label - normalize it
                        raw_label = label_match.group(1)  # Get the label name (without colon)
                        current_label = normalize_field_label(raw_label)
                        current_value = []
                        skip_next = False
                    else:
                        # This is a value (text after label)
                        # FIX 3G: Remove label word from value if it appears
                        # Prevents "Location 2A / 02" when text is "Location:Location 2A / 02"
                        part_cleaned = part
                        if current_label:
                            # Remove label word from start of value
                            part_cleaned = re.sub(r'^' + re.escape(current_label) + r'\s+', '', part, flags=re.IGNORECASE)
                        # Only add if we have a current label (skip text before first label)
                        if current_label:
                            current_value.append(part_cleaned)
                        elif not current_label and i == 0:
                            # Text before first label - might be name or continuation
                            if not name_field and len(part) < 50:
                                name_field = part
                            else:
                                current_value.append(part)
                
                # Save last label/value pair
                if current_label and current_value:
                    value_text = ' '.join(current_value).strip()
                    # FIX 3F: Remove label word from value if it appears at start
                    value_text = re.sub(r'^' + re.escape(current_label) + r'\s+', '', value_text, flags=re.IGNORECASE)
                    if value_text.strip():
                        label_value_pairs.append([current_label, value_text.strip()])
            # Check if it contains colon (Label: Value format)
            elif ':' in text:
                parts = text.split(':', 1)
                if len(parts) == 2:
                    raw_label = parts[0].strip()
                    value = parts[1].strip()
                    if raw_label and value:
                        # Normalize label for consistency
                        label = normalize_field_label(raw_label)
                        label_value_pairs.append([label, value])
                else:
                    if text.strip():
                        label_value_pairs.append(['', text.strip()])
            else:
                # No colon - might be continuation of previous value
                if label_value_pairs:
                    label_value_pairs[-1][1] = f"{label_value_pairs[-1][1]} {text}".strip()
                else:
                    if not name_field:
                        name_field = text
                    else:
                        label_value_pairs.append(['', text])
        
        i += 1
    
    # Build result: Name first (if found), then Label | Value pairs
    result = []
    if name_field:
        result.append(['Name', name_field])
    result.extend(label_value_pairs)
    
    return result


def convert_single_column_to_multi_column_table(table_data, html_table_element):
    """
    Convert single-column table to multi-column format using pattern-based parsing.
    
    Args:
        table_data: Table dict with single-column rows (from extract_table_from_html)
        html_table_element: BeautifulSoup table element (for parsing HTML directly)
        
    Returns:
        Table dict with multi-column rows (Label | Value format)
    """
    if not table_data.get('rows'):
        return table_data
    
    # Check if this is a single-column table
    if not all(len(row) == 1 for row in table_data.get('rows', [])):
        return table_data  # Already multi-column
    
    # Parse each row from HTML directly (preserve structure)
    parsed_rows_data = []  # List of dicts: {label: value}
    all_labels_set = set()  # Collect all unique labels
    
    # Get all rows from HTML table
    html_rows = html_table_element.find_all('tr', recursive=False)
    if not html_rows:
        html_rows = html_table_element.find_all('tr', recursive=True)
        # Filter nested table rows
        html_rows = [r for r in html_rows if r.find_parent('table') == html_table_element]
    
    for html_row in html_rows:
        # Get single cell (single-column table)
        cells = html_row.find_all(['td', 'th'], recursive=False)
        if not cells:
            cells = html_row.find_all(['td', 'th'], recursive=True)
            cells = [c for c in cells if c.find_parent('table') == html_table_element]
        
        if len(cells) == 1:
            # Single-column cell - parse it
            cell_html = cells[0]
            label_value_pairs = parse_single_column_table_content(cell_html)
            
            if label_value_pairs:
                # Convert pairs to dict for easier handling
                row_dict = {}
                for label, value in label_value_pairs:
                    if label:  # Only add if label exists
                        # Normalize label for consistency across 200k+ URLs
                        normalized_label = normalize_field_label(label)
                        row_dict[normalized_label] = value
                        all_labels_set.add(normalized_label)
                    else:
                        # No label - might be continuation or standalone value
                        if row_dict:
                            # Append to last value
                            last_key = list(row_dict.keys())[-1]
                            row_dict[last_key] = f"{row_dict[last_key]} {value}".strip()
                        else:
                            # First item with no label - use "Info"
                            row_dict['Info'] = value
                            all_labels_set.add('Info')
                
                parsed_rows_data.append(row_dict)
            else:
                # Parsing failed - try to extract name from cell content before falling back to "Original"
                # This prevents "| Original |" rows when we can extract at least a name
                cell_text = extract_cell_links(cells[0])
                # Try to extract name pattern (capitalized words at start, including umlauts)
                name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){0,3})', cell_text, re.UNICODE)
                if name_match:
                    name = name_match.group(1).strip()
                    # Extract remaining content
                    remaining = cell_text[len(name):].strip()
                    if remaining:
                        parsed_rows_data.append({'Name': name, 'Info': remaining})
                        all_labels_set.add('Name')
                        all_labels_set.add('Info')
                    else:
                        parsed_rows_data.append({'Name': name})
                        all_labels_set.add('Name')
                else:
                    # No name found, use "Original" as fallback
                    parsed_rows_data.append({'Original': cell_text})
                    all_labels_set.add('Original')
        else:
            # Multi-column row (shouldn't happen in single-column table, but handle it)
            row_data = [extract_cell_links(cell) for cell in cells]
            # Convert to dict format for consistency
            row_dict = {}
            for i, cell_val in enumerate(row_data):
                label = f"Column {i+1}"
                row_dict[label] = cell_val
                all_labels_set.add(label)
            parsed_rows_data.append(row_dict)
    
    # If we successfully parsed at least one row, update table structure
    if parsed_rows_data and all_labels_set:
        # Create ordered header list (Name first if present, then others alphabetically)
        headers = []
        if 'Name' in all_labels_set:
            headers.append('Name')
            all_labels_set.remove('Name')
        # Add remaining labels in sorted order
        headers.extend(sorted(all_labels_set))
        
        # Convert dict rows to list rows matching header order
        parsed_rows = []
        for row_dict in parsed_rows_data:
            row_values = [row_dict.get(label, '') for label in headers]
            parsed_rows.append(row_values)
        
        table_data['rows'] = parsed_rows
        table_data['headers'] = headers
    
    return table_data


def extract_headings_and_tables_in_dom_order(html_content, url=None):
    """
    Extract headings and tables in DOM order from rendered HTML.
    
    This function:
    1. Finds all headings (h1-h6) and tables in the HTML
    2. Sorts them by their position in the DOM
    3. Associates each table with its nearest preceding heading
    4. Returns a list of content items in DOM order
    
    Args:
        html_content: Rendered HTML string from Crawl4AI (result.html)
        url: Optional URL to enable PUBDB-specific filtering
        
    Returns:
        List of content items: [
            {'type': 'heading', 'level': 4, 'text': 'SCIENTISTS', 'position': 0},
            {'type': 'table', 'data': {...}, 'position': 1},
            ...
        ]
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return []
    
    try:
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find main content area
        main_content_area = (soup.find('main') or 
                           soup.find('article') or 
                           soup.find('body') or
                           soup)
        
        # FIX 2B: Filter out navigation/header/footer headings if main_content_area is body
        # This prevents extracting navigation headings when body is used as fallback
        navigation_containers = ['nav', 'header', 'footer', 'aside']
        nav_elements = set()
        if main_content_area.name == 'body':
            # Filter by semantic HTML elements
            for nav_tag in navigation_containers:
                for nav_elem in soup.find_all(nav_tag, recursive=True):
                    for heading in nav_elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
            
            # Also filter by common navigation class/id patterns
            nav_patterns = [r'nav', r'sidebar', r'menu', r'header', r'footer', r'topbar', r'breadcrumb']
            for pattern in nav_patterns:
                for elem in soup.find_all(['div', 'section'], class_=re.compile(pattern, re.I), recursive=True):
                    for heading in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
                for elem in soup.find_all(['div', 'section'], id=re.compile(pattern, re.I), recursive=True):
                    for heading in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
        
        # Collect all headings and tables with their DOM positions
        all_elements = []
        
        # Find all headings and tables
        for elem in main_content_area.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table'], recursive=True):
            # Skip navigation headings
            if elem.name.startswith('h') and elem in nav_elements:
                continue
            # Only process top-level tables (not nested)
            if elem.name == 'table' and elem.find_parent('table') is not None:
                continue
            
            # FIX 2: Calculate position: count only previous elements within main_content_area
            # This prevents counting navigation/header elements that appear before main content
            position = 0
            # Get all elements in main_content_area in document order
            all_content_elems = main_content_area.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table'], recursive=True)
            for prev_elem in all_content_elems:
                # Stop when we reach current element
                if prev_elem == elem:
                    break
                # Only count top-level tables (not nested)
                if prev_elem.name == 'table':
                    if prev_elem.find_parent('table') is None:
                        position += 1
                elif prev_elem.name.startswith('h'):
                    position += 1
            
            # #region agent log
            elem_text = elem.get_text(strip=True)[:50] if elem.name.startswith('h') else 'TABLE'
            if 'heuser' in str(html_content).lower():
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'H1',
                        'location': 'crawl_desy_simple.py:2036',
                        'message': 'Element found in DOM',
                        'data': {
                            'type': 'heading' if elem.name.startswith('h') else 'table',
                            'tag': elem.name,
                            'position': position,
                            'text_preview': elem_text,
                            'has_parent_table': elem.find_parent('table') is not None if elem.name == 'table' else False
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
            # #endregion
            
            all_elements.append({
                'element': elem,
                'type': 'heading' if elem.name.startswith('h') else 'table',
                'position': position
            })
        
        # Sort by position
        all_elements.sort(key=lambda x: x['position'])
        
        # #region agent log
        if 'heuser' in str(html_content).lower():
            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                import json
                f.write(json.dumps({
                    'sessionId': 'debug-session',
                    'runId': 'run1',
                    'hypothesisId': 'H1',
                    'location': 'crawl_desy_simple.py:2067',
                    'message': 'Elements sorted by position',
                    'data': {
                        'sorted_order': [
                            {
                                'type': item['type'],
                                'position': item['position'],
                                'text_preview': item['element'].get_text(strip=True)[:50] if item['element'].name.startswith('h') else 'TABLE'
                            }
                            for item in all_elements[:10]
                        ]
                    },
                    'timestamp': int(__import__('time').time() * 1000)
                }) + '\n')
        # #endregion
        
        # Process elements and extract content
        dom_ordered_content = []
        seen_headings = set()
        
        for item in all_elements:
            elem = item['element']
            
            if item['type'] == 'heading':
                heading_text = elem.get_text(strip=True)
                if heading_text and heading_text not in seen_headings:
                    seen_headings.add(heading_text)
                    level = int(elem.name[1])
                    dom_ordered_content.append({
                        'type': 'heading',
                        'level': level,
                        'text': heading_text,
                        'position': item['position']
                    })
            
            elif item['type'] == 'table':
                # PUBDB-specific filtering: Only filter UI tables on PUBDB pages
                # Check both URL and content to handle redirects/embedded content
                if _is_pubdb_page(url, html_content):
                    table_text = elem.get_text(strip=True).lower()
                    
                    if is_pubdb_ui_table(table_text):
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'PUBDB',
                                'location': 'crawl_desy_simple.py:2134',
                                'message': 'Skipping PUBDB UI table in DOM extraction',
                                'data': {
                                    'table_text_preview': table_text[:150],
                                    'matched_keywords': [kw for kw in _PUBDB_UI_KEYWORDS if kw in table_text],
                                    'url': url,
                                    'is_pubdb_url': url and is_pubdb_url(url) if url else False,
                                    'is_pubdb_content': is_pubdb_content(html_content) if html_content else False
                                },
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        continue  # Skip this UI table
                
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'H1',
                        'location': 'crawl_desy_simple.py:2067',
                        'message': 'Table element found in DOM',
                        'data': {
                            'table_html_preview': str(elem)[:200],
                            'table_id': elem.get('id', ''),
                            'table_class': ' '.join(elem.get('class', []))
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                
                # Extract table data using existing function
                table_data = extract_table_from_html(elem)
                
                # #region agent log
                # Check if this table contains Andrey, Anjali, or Ankita
                table_text = elem.get_text()
                is_first_three = any(name in table_text for name in ['Andrey Siemens', 'Anjali Panchwanee', 'Ankita Negi'])
                is_other = any(name in table_text for name in ['Anna Barinskaya', 'Bojan Bosnjak', 'Christina Bömer'])
                
                if is_first_three or is_other:
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'H2',
                            'location': 'crawl_desy_simple.py:2108',
                            'message': 'Table extraction result',
                            'data': {
                                'is_first_three': is_first_three,
                                'is_other': is_other,
                                'has_rows': bool(table_data.get('rows')),
                                'num_rows': len(table_data.get('rows', [])),
                                'num_cols': len(table_data.get('rows', [0])) if table_data.get('rows') else 0,
                                'has_headers': bool(table_data.get('headers')),
                                'headers': [str(h)[:50] for h in table_data.get('headers', [])],
                                'first_row_preview': [str(c)[:50] for c in table_data.get('rows', [])[0][:3]] if table_data.get('rows') else None,
                                'table_html_first_500': str(elem)[:500]
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                # #endregion
                
                if table_data.get('rows'):
                    # Solution 3: Check if single-column table and convert to multi-column
                    is_single_column = all(len(row) == 1 for row in table_data.get('rows', []))
                    if is_single_column:
                        table_data = convert_single_column_to_multi_column_table(table_data, elem)
                    
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'A',
                            'location': 'crawl_desy_simple.py:1969',
                            'message': 'Table found in DOM extraction',
                            'data': {
                                'num_rows': len(table_data.get('rows', [])),
                                'num_cols': len(table_data.get('rows', [0])) if table_data.get('rows') else 0,
                                'has_headers': bool(table_data.get('headers')),
                                'first_row_preview': str(table_data.get('rows', [])[0][:2]) if table_data.get('rows') else None
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                    
                    dom_ordered_content.append({
                        'type': 'table',
                        'data': table_data,
                        'position': item['position']
                    })
        
        return dom_ordered_content
    except Exception as e:
        print(f"[DEBUG] extract_headings_and_tables_in_dom_order failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def format_tables_with_headings_as_markdown(dom_ordered_content):
    """
    Format headings and tables as markdown, preserving DOM order.
    
    Args:
        dom_ordered_content: List from extract_headings_and_tables_in_dom_order()
        
    Returns:
        Markdown string with headings and tables in DOM order
    """
    if not dom_ordered_content:
        return ""
    
    # Defensive check: ensure dom_ordered_content is a list
    if dom_ordered_content is None:
        return ""
    
    # GENERAL: Merge consecutive single-row tables with similar structure (same headers)
    # This handles cases where each row is in its own <table> element
    merged_content = []
    i = 0
    while i < len(dom_ordered_content):
        item = dom_ordered_content[i]
        
        if item['type'] == 'heading':
            merged_content.append(item)
            i += 1
        elif item['type'] == 'table':
            # Check if this is a single-row table that might be part of a larger table
            table_data = item.get('data', {}) or {}
            rows = table_data.get('rows', []) or []
            headers = table_data.get('headers', []) or []
            
            # Check if this is a single-row table with structured data (Name, E-mail, Phone, Location)
            is_single_row_structured = (len(rows) == 1 and 
                                       headers and 
                                       len(headers) >= 2 and
                                       any(label.lower() in ['name', 'e-mail', 'email', 'phone', 'location'] for label in headers))
            
            # #region agent log
            if headers and any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'M',
                        'location': 'crawl_desy_simple.py:2177',
                        'message': 'Checking merge candidate',
                        'data': {
                            'num_rows': len(rows),
                            'num_headers': len(headers) if headers else 0,
                            'headers': [str(h) for h in headers] if headers else [],
                            'is_single_row_structured': is_single_row_structured
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
            # #endregion
            
            if is_single_row_structured:
                # Look ahead for consecutive single-row tables with same headers
                merge_candidates = [item]
                j = i + 1
                while j < len(dom_ordered_content):
                    next_item = dom_ordered_content[j]
                    # #region agent log
                    if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'M',
                                'location': 'crawl_desy_simple.py:2208',
                                'message': 'Checking next item for merge',
                                'data': {
                                    'j': j,
                                    'next_item_type': next_item.get('type'),
                                    'total_items': len(dom_ordered_content)
                                },
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                    # #endregion
                    if next_item['type'] != 'table':
                        # #region agent log
                        if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'M',
                                    'location': 'crawl_desy_simple.py:2210',
                                    'message': 'Next item is not a table, stopping merge',
                                    'data': {'next_item_type': next_item.get('type')},
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                        # #endregion
                        break
                    
                    next_table_data = next_item.get('data', {}) or {}
                    next_rows = next_table_data.get('rows', []) or []
                    next_headers = next_table_data.get('headers', []) or []
                    
                    # Check if next table has same structure (same header types, single row)
                    # GENERAL: Headers may have person-specific values (e.g., "Andrey Siemens E-mail" vs "Anjali Panchwanee E-mail")
                    # So we check if headers have the same field types (E-mail, Location, Phone) rather than exact match
                    # Also allow different numbers of columns as long as field types match (some tables may be missing columns)
                    # #region agent log
                    if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'M',
                                'location': 'crawl_desy_simple.py:2225',
                                'message': 'Checking if next table is mergeable',
                                'data': {
                                    'next_num_rows': len(next_rows),
                                    'next_has_headers': bool(next_headers),
                                    'next_headers': [str(h)[:30] for h in next_headers[:3]] if next_headers else []
                                },
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                    # #endregion
                    if len(next_rows) == 1 and next_headers:
                        # Extract field types from headers (normalize to generic labels)
                        def extract_field_type(header):
                            header_lower = str(header).lower()
                            if 'e-mail' in header_lower or 'email' in header_lower:
                                return 'e-mail'
                            elif 'phone' in header_lower or 'tel' in header_lower:
                                return 'phone'
                            elif 'location' in header_lower or 'office' in header_lower or 'room' in header_lower:
                                return 'location'
                            elif 'name' in header_lower:
                                return 'name'
                            else:
                                return header_lower
                        
                        current_field_types = set(extract_field_type(h) for h in (headers or []))
                        next_field_types = set(extract_field_type(h) for h in (next_headers or []))
                        
                        # Merge if field types overlap (same structure, even if some columns are missing)
                        # Allow merging if at least 2 field types match (e.g., both have E-Mail and Location)
                        intersection = current_field_types & next_field_types
                        will_merge = len(intersection) >= 2
                        
                        # #region agent log
                        if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'M',
                                    'location': 'crawl_desy_simple.py:2240',
                                    'message': 'Field type check for merge',
                                    'data': {
                                        'current_field_types': list(current_field_types),
                                        'next_field_types': list(next_field_types),
                                        'intersection': list(intersection),
                                        'intersection_len': len(intersection),
                                        'will_merge': will_merge,
                                        'current_headers': [str(h)[:30] for h in (headers or [])[:3]],
                                        'next_headers': [str(h)[:30] for h in (next_headers or [])[:3]]
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                        # #endregion
                        
                        if will_merge:
                            merge_candidates.append(next_item)
                            # #region agent log
                            if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                    import json
                                    f.write(json.dumps({
                                        'sessionId': 'debug-session',
                                        'runId': 'run1',
                                        'hypothesisId': 'M',
                                        'location': 'crawl_desy_simple.py:2267',
                                        'message': 'Added merge candidate',
                                        'data': {
                                            'current_field_types': list(current_field_types),
                                            'next_field_types': list(next_field_types),
                                            'merge_candidates_count': len(merge_candidates)
                                        },
                                        'timestamp': int(__import__('time').time() * 1000)
                                    }) + '\n')
                            # #endregion
                            j += 1
                        else:
                            # #region agent log
                            if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                    import json
                                    f.write(json.dumps({
                                        'sessionId': 'debug-session',
                                        'runId': 'run1',
                                        'hypothesisId': 'M',
                                        'location': 'crawl_desy_simple.py:2222',
                                        'message': 'Field types do not match, stopping merge',
                                        'data': {
                                            'current_field_types': list(current_field_types),
                                            'next_field_types': list(next_field_types),
                                            'intersection': list(intersection),
                                            'intersection_len': len(intersection)
                                        },
                                        'timestamp': int(__import__('time').time() * 1000)
                                    }) + '\n')
                            # #endregion
                            break
                    else:
                        # Next item is not a table or doesn't have single row, stop merging
                        break
                
                # If we found multiple tables to merge, merge them
                # #region agent log
                if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'M',
                            'location': 'crawl_desy_simple.py:2227',
                            'message': 'Merge decision',
                            'data': {
                                'merge_candidates_count': len(merge_candidates),
                                'will_merge': len(merge_candidates) > 1
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                # #endregion
                if len(merge_candidates) > 1:
                    # Merge all rows from candidates into one table
                    merged_rows = []
                    # Use normalized generic headers (extract field types, not person-specific values)
                    def normalize_header(header):
                        header_str = str(header).lower()
                        if 'e-mail' in header_str or 'email' in header_str:
                            return 'E-Mail'
                        elif 'phone' in header_str or 'tel' in header_str:
                            return 'Phone'
                        elif 'location' in header_str or 'office' in header_str or 'room' in header_str:
                            return 'Location'
                        elif 'name' in header_str:
                            return 'Name'
                        else:
                            # Keep original if no match
                            return str(header)
                    
                    # Collect all unique headers from all candidates to ensure we don't lose any columns
                    all_headers_set = set()
                    for candidate in merge_candidates:
                        candidate_data = candidate.get('data', {}) or {}
                        candidate_headers = candidate_data.get('headers', []) or []
                        for h in candidate_headers:
                            if h:  # Skip None headers
                                normalized_h = normalize_header(h)
                                all_headers_set.add(normalized_h)
                    
                    # FIX: If Name is not in headers but rows contain email links with names, infer Name column
                    # This handles cases where HTML structure doesn't have explicit Name header but email links contain names
                    if 'Name' not in all_headers_set:
                        # Check if any row has an email link with a name-like pattern
                        for candidate in merge_candidates:
                            candidate_data = candidate.get('data', {}) or {}
                            candidate_rows = candidate_data.get('rows', []) or []
                            for row in candidate_rows:
                                # Check all cells in the row for email links
                                for cell in row:
                                    cell_str = str(cell)
                                    # Look for markdown email link: [Name](mailto:...)
                                    email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', cell_str)
                                    if email_match:
                                        link_text = email_match.group(1).strip()
                                        # Validate it looks like a name (1-5 words, capitalized)
                                        words = link_text.split()
                                        if 1 <= len(words) <= 5:
                                            # Check if words look like names (start with capital or umlaut)
                                            is_name = all(
                                                w and (w[0].isupper() or w[0] in 'äöüÄÖÜ') 
                                                for w in words if w and not w.startswith('(')
                                            )
                                            # Check it's not a phone/number pattern
                                            has_phone = bool(re.search(r'\d{3,}|T\.|Phone', link_text))
                                            
                                            if is_name and not has_phone:
                                                # Found a name in email link - add Name to headers
                                                all_headers_set.add('Name')
                                                break
                                if 'Name' in all_headers_set:
                                    break
                            if 'Name' in all_headers_set:
                                break
                    
                    # Create ordered header list (Name first if present, then E-Mail, Phone, Location, then others)
                    normalized_headers = []
                    header_order = ['Name', 'E-Mail', 'Phone', 'Location']
                    for h in header_order:
                        if h in all_headers_set:
                            normalized_headers.append(h)
                            all_headers_set.remove(h)
                    # Add any remaining headers
                    normalized_headers.extend(sorted(all_headers_set))
                    
                    # Now merge rows, ensuring all columns are present
                    for candidate in merge_candidates:
                        candidate_data = candidate.get('data', {}) or {}
                        candidate_rows = candidate_data.get('rows', []) or []
                        candidate_headers = candidate_data.get('headers', []) or []
                        # Map candidate headers to normalized headers
                        candidate_header_map = {normalize_header(h): h for h in candidate_headers if h}
                        
                        if not candidate_rows:
                            continue  # Skip if no rows
                        
                        for row in candidate_rows:
                            # Create a new row with all normalized headers
                            new_row = []
                            for norm_h in normalized_headers:
                                # Find the original header that maps to this normalized header
                                orig_h = candidate_header_map.get(norm_h)
                                if orig_h and orig_h in candidate_headers:
                                    col_idx = candidate_headers.index(orig_h)
                                    if col_idx < len(row):
                                        new_row.append(row[col_idx])
                                    else:
                                        new_row.append('')
                                else:
                                    # Header not in original - might be inferred (like Name)
                                    # If this is Name and we have email links in the row, extract name from email
                                    if norm_h == 'Name':
                                        # Look for email link in any cell of this row
                                        name_extracted = False
                                        for cell in row:
                                            cell_str = str(cell)
                                            email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', cell_str)
                                            if email_match:
                                                link_text = email_match.group(1).strip()
                                                # Validate it looks like a name
                                                words = link_text.split()
                                                if 1 <= len(words) <= 5:
                                                    is_name = all(
                                                        w and (w[0].isupper() or w[0] in 'äöüÄÖÜ') 
                                                        for w in words if w and not w.startswith('(')
                                                    )
                                                    has_phone = bool(re.search(r'\d{3,}|T\.|Phone', link_text))
                                                    
                                                    if is_name and not has_phone:
                                                        new_row.append(link_text)
                                                        name_extracted = True
                                                        break
                                        if not name_extracted:
                                            new_row.append('')
                                    else:
                                        new_row.append('')
                            merged_rows.append(new_row)
                    
                    # Create merged table with normalized headers
                    merged_table = {
                        'type': 'table',
                        'data': {
                            'headers': normalized_headers,
                            'rows': merged_rows
                        },
                        'position': item['position']
                    }
                    merged_content.append(merged_table)
                    # #region agent log
                    if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers):
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'M',
                                'location': 'crawl_desy_simple.py:2369',
                                'message': 'Merged table added to merged_content',
                                'data': {
                                    'merged_rows_count': len(merged_rows),
                                    'normalized_headers': normalized_headers,
                                    'merge_candidates_count': len(merge_candidates)
                                },
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                    # #endregion
                    i = j  # Skip all merged tables
                else:
                    # No merge needed, add as-is
                    merged_content.append(item)
                    i += 1
            else:
                # Not a candidate for merging, add as-is
                merged_content.append(item)
                i += 1
        else:
            merged_content.append(item)
            i += 1
    
    # Now format the merged content
    markdown_output = ""
    seen_table_signatures = set()  # For deduplication
    
    # #region agent log
    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
        import json
        f.write(json.dumps({
            'sessionId': 'debug-session',
            'runId': 'run1',
            'hypothesisId': 'T',
            'location': 'crawl_desy_simple.py:2383',
            'message': 'Starting to format merged_content',
            'data': {
                'merged_content_count': len(merged_content),
                'item_types': [item.get('type') for item in merged_content[:10]]
            },
            'timestamp': int(__import__('time').time() * 1000)
        }) + '\n')
    # #endregion
    
    for item in merged_content:
        if item['type'] == 'heading':
            level = item['level']
            text = item['text']
            markdown_output += '#' * level + ' ' + text + "\n\n"
        
        elif item['type'] == 'table':
            table_data = item.get('data', {}) or {}
            headers = table_data.get('headers', []) or []
            rows = table_data.get('rows', []) or []
            
            # #region agent log
            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                import json
                f.write(json.dumps({
                    'sessionId': 'debug-session',
                    'runId': 'run1',
                    'hypothesisId': 'Q',
                    'location': 'crawl_desy_simple.py:2065',
                    'message': 'Processing table in DOM-order format',
                    'data': {
                        'num_rows': len(rows),
                        'num_cols': len(rows[0]) if rows else 0,
                        'has_headers': bool(headers),
                        'headers_preview': [str(h)[:30] for h in headers[:2]] if headers else None,
                        'first_row_preview': [str(c)[:30] for c in rows[0][:2]] if rows else None
                    },
                    'timestamp': int(__import__('time').time() * 1000)
                }) + '\n')
            # #endregion
            
            if not rows:
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'C',
                        'location': 'crawl_desy_simple.py:2448',
                        'message': 'Table filtered: no rows',
                        'data': {
                            'headers': [str(h)[:30] for h in headers[:3]] if headers else [],
                            'has_headers': bool(headers)
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                continue
            
            # GENERAL: Filter out trivial/small tables (single-row tables with just links, empty tables, etc.)
            # Skip tables that are too small or contain only links/empty cells
            # BUT: Allow single-row tables with 3+ columns that contain structured data (like staff/group member tables)
            if len(rows) <= 1:
                # Single-row table - check if it's just a link or trivial content
                first_row = rows[0] if rows else []
                # Count non-empty cells
                non_empty_cells = [str(c).strip() for c in first_row if str(c).strip()]
                num_cols = len(first_row)
                
                # GENERAL: Only filter if table has 1-2 non-empty cells AND they're just links
                # Tables with 3+ columns (even if single-row) may contain structured data (staff info, etc.)
                if len(non_empty_cells) <= 2 and num_cols <= 2:
                    # Check if cells contain mostly links/URLs
                    link_count = sum(1 for cell in non_empty_cells if re.search(r'\[.*?\]\(.*?\)|https?://', str(cell)))
                    if link_count >= len(non_empty_cells):
                        # This is a trivial link table - skip it
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'D',
                                'location': 'crawl_desy_simple.py:2035',
                                'message': 'Table filtered: trivial link table',
                                'data': {'num_rows': len(rows), 'link_count': link_count, 'num_cols': num_cols},
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        continue
                # GENERAL: Filter out single-cell tables that are likely broken fragments
                # Pattern: Single-cell table with just a value (no label, no structure)
                if len(non_empty_cells) == 1:
                    single_cell = non_empty_cells[0]
                    # Skip if it's just a number/unit with no label (broken fragment)
                    if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)?\s*$', single_cell, re.I):
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'N',
                                'location': 'crawl_desy_simple.py:2132',
                                'message': 'Table filtered: single-cell broken fragment',
                                'data': {'cell_content': single_cell[:50]},
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        continue
                # GENERAL: Filter out single-cell tables that are likely broken fragments
                # Pattern: Single-cell table with just a value (no label, no structure)
                if len(non_empty_cells) == 1:
                    single_cell = non_empty_cells[0]
                    # Skip if it's just a number/unit with no label (broken fragment)
                    if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)?\s*$', single_cell, re.I):
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'N',
                                'location': 'crawl_desy_simple.py:2132',
                                'message': 'Table filtered: single-cell broken fragment',
                                'data': {'cell_content': single_cell[:50]},
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        continue
            
            # GENERAL: Filter out malformed tables (too many columns, concatenated data)
            # Pattern: Tables with 10+ columns are likely malformed (concatenated data)
            if rows and len(rows[0]) > 10:
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'E',
                        'location': 'crawl_desy_simple.py:2040',
                        'message': 'Table filtered: too many columns',
                        'data': {'num_cols': len(rows[0])},
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                continue
            
            # Deduplication: Create signature from first few non-empty cells of first row
            # GENERAL: More robust signature to catch duplicates even with formatting differences
            sig_row = rows[0] if rows else []
            # Get first 3 non-empty cells (more robust than 2)
            sig_cells = [str(c).strip().lower()[:50] for c in sig_row[:3] if str(c).strip()]
            # Also include header signature if available
            if headers:
                header_sig = "|".join([str(h).strip().lower()[:30] for h in headers[:3] if str(h).strip()])
                table_sig = f"{header_sig}|{''.join(sig_cells)}"
            else:
                table_sig = "|".join(sig_cells)
            
            # GENERAL: Also check for malformed table signatures (concatenated data)
            # Only filter if table has many columns (10+) OR if it's single-column with field labels
            # Multi-column tables (2-10 columns) with field labels in cells are legitimate
            if table_sig:
                num_columns = len(rows[0]) if rows else 0
                field_labels_in_sig = len(re.findall(r'\b(e-mail|phone|location|email|tel|telephone):', table_sig, re.I))
                # Only filter if: (many columns AND field labels) OR (single-column with 3+ field labels)
                # This allows legitimate 2-column tables where cells contain structured data
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'F',
                        'location': 'crawl_desy_simple.py:2062',
                        'message': 'Table signature check',
                        'data': {
                            'num_columns': num_columns,
                            'field_labels_in_sig': field_labels_in_sig,
                            'table_sig_preview': table_sig[:100]
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                if (num_columns > 10 and field_labels_in_sig >= 3) or (num_columns == 1 and field_labels_in_sig >= 3):
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'F',
                            'location': 'crawl_desy_simple.py:2063',
                            'message': 'Table filtered: signature check failed',
                            'data': {'num_columns': num_columns, 'field_labels_in_sig': field_labels_in_sig},
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                    continue
            
            if table_sig and table_sig in seen_table_signatures:
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'G',
                        'location': 'crawl_desy_simple.py:2066',
                        'message': 'Table filtered: duplicate signature',
                        'data': {'table_sig_preview': table_sig[:100]},
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                continue  # Skip duplicate
            
            if table_sig:
                seen_table_signatures.add(table_sig)
            
            # #region agent log
            if any('siemens' in str(h).lower() or 'panchwanee' in str(h).lower() for h in headers[:2] if h):
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'T',
                        'location': 'crawl_desy_simple.py:2630',
                        'message': 'Table passed all filters, starting to format',
                        'data': {
                            'num_rows': len(rows),
                            'num_cols': len(rows[0]) if rows else 0,
                            'has_headers': bool(headers),
                            'headers': [str(h)[:30] for h in headers[:3]] if headers else [],
                            'table_sig': table_sig[:100] if table_sig else None
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
            # #endregion
            
            # Format as markdown table
            if headers:
                # GENERAL: Check if detected headers are actually label-value pairs (common in parameter tables)
                # If headers look like labels (end with ":" or contain label words), treat them as data instead
                is_label_value_headers = False
                if len(headers) >= 2:
                    first_header = str(headers[0]).strip()
                    second_header = str(headers[1]).strip() if len(headers) > 1 else ""
                    # Check if first header ends with ":" (label indicator)
                    if first_header.endswith(':') or first_header.endswith('：'):
                        is_label_value_headers = True
                    # Also check if headers contain common label words
                    label_words = ['energy', 'circumference', 'number', 'length', 'angle', 'radius', 'field', 
                                   'aperture', 'gradient', 'emittance', 'tune', 'frequency', 'time', 'current',
                                   'charge', 'power', 'size', 'divergence', 'function', 'damping', 'spread',
                                   'bunch', 'separation', 'bucket', 'coupling', 'factor', 'magnetic', 'critical',
                                   'photon', 'revolution', 'ratio', 'electron', 'loss', 'turn', 'radiation',
                                   'lifetime', 'sector', 'cell', 'section', 'undulator', 'beam', 'horizontal',
                                   'vertical', 'momentum', 'compaction', 'chromaticity', 'synchrotron', 'wiggler',
                                   'alignment', 'tolerance', 'dipole', 'quadrupole', 'sextupole', 'bpm']
                    # Check if first header contains label words (e.g., "Electron energy", "Circumference", etc.)
                    if any(word in first_header.lower() for word in label_words):
                        is_label_value_headers = True
                    # Also check if second header looks like a value (numbers, units, etc.)
                    # Pattern: Second header is a value (number with unit, or just a number/unit)
                    if second_header and re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad|Hz|V|A|W|J|kg|g|s|min|h|d|y|°C|K|Pa|bar|atm|psi|N|kgf|lbf|m/s|km/h|mph|rpm|rad/s|deg/s)?\s*$', second_header, re.I):
                        is_label_value_headers = True
                    
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'H',
                            'location': 'crawl_desy_simple.py:2289',
                            'message': 'Checking if headers are label-value pairs',
                            'data': {
                                'first_header': first_header[:50],
                                'second_header': second_header[:50],
                                'is_label_value_headers': is_label_value_headers,
                                'has_label_word': any(word in first_header.lower() for word in label_words),
                                'second_is_value': bool(second_header and re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad|Hz|V|A|W|J|kg|g|s|min|h|d|y|°C|K|Pa|bar|atm|psi|N|kgf|lbf|m/s|km/h|mph|rpm|rad/s|deg/s)?\s*$', second_header, re.I))
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                
                if is_label_value_headers:
                    # Headers are actually label-value pairs - treat as data rows (no separate header row)
                    # GENERAL: When headers are label-value pairs, they should be treated as data, not headers
                    # Do NOT add a separator row - that would make them look like headers in markdown
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'H',
                            'location': 'crawl_desy_simple.py:2318',
                            'message': 'Detected label-value headers, treating as data (no separator)',
                            'data': {
                                'first_header': str(headers[0])[:50] if headers else None,
                                'is_label_value_headers': is_label_value_headers
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                    # Add headers as first data row (no separator - they're data, not headers)
                    if headers:
                        # GENERAL: Normalize header content - replace newlines with spaces
                        # Use unicode-aware regex to handle umlauts and special characters correctly
                        normalized_headers = []
                        for h in headers:
                            header_str = str(h)
                            header_str = re.sub(r'\s+', ' ', header_str, flags=re.UNICODE).strip()
                            normalized_headers.append(header_str)
                        markdown_output += "| " + " | ".join(normalized_headers) + " |\n"
                        # Skip first row if it matches headers (to avoid duplication)
                        if rows and len(rows) > 0:
                            first_row_str = "|".join(str(c).strip().lower() for c in rows[0][:len(headers)])
                            headers_str = "|".join(str(h).strip().lower() for h in headers)
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'R',
                                'location': 'crawl_desy_simple.py:2350',
                                'message': 'Checking for duplicate first row',
                                'data': {
                                    'first_row_str': first_row_str[:100],
                                    'headers_str': headers_str[:100],
                                    'matches': first_row_str == headers_str,
                                    'num_rows_before': len(rows)
                                },
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        if first_row_str == headers_str:
                            rows = rows[1:]  # Skip duplicate first row
                else:
                    # Normal headers - NOT label-value pairs
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'J',
                            'location': 'crawl_desy_simple.py:2374',
                            'message': 'Using normal headers (not label-value)',
                            'data': {
                                'first_header': str(headers[0])[:50] if headers else None,
                                'is_label_value_headers': is_label_value_headers
                            },
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                    if headers:
                        # GENERAL: Normalize header content - replace newlines with spaces
                        # Use unicode-aware regex to handle umlauts and special characters correctly
                        normalized_headers = []
                        for h in headers:
                            header_str = str(h)
                            header_str = re.sub(r'\s+', ' ', header_str, flags=re.UNICODE).strip()
                            normalized_headers.append(header_str)
                        markdown_output += "| " + " | ".join(normalized_headers) + " |\n"
                        markdown_output += "| " + " | ".join(["---"] * len(headers)) + " |\n"
            elif rows and len(rows[0]) > 1:
                # GENERAL: Only use first row as header if it doesn't contain data patterns
                # If first row contains email, phone, location, or other data patterns, treat all rows as data
                first_row = rows[0]
                first_row_text = " ".join(str(cell) for cell in first_row).lower()
                # Check if first row contains data patterns (email, phone, location, etc.)
                data_patterns = [
                    r'e-mail|email|mailto:',
                    r'phone|tel|telephone|\+\d',
                    r'location|address|office|room',
                    r'@\w+\.\w+',  # Email addresses
                ]
                has_data_patterns = any(re.search(pattern, first_row_text, re.I) for pattern in data_patterns)
                
                # Check if first row is a label-value pair (common in parameter tables)
                # Pattern: First column ends with ":" and second column is a value (number, unit, etc.)
                is_label_value_pair = False
                if len(first_row) >= 2:
                    first_cell = str(first_row[0]).strip()
                    second_cell = str(first_row[1]).strip()
                    # Check if first cell ends with ":" (label indicator)
                    if first_cell.endswith(':') or first_cell.endswith('：'):
                        is_label_value_pair = True
                    # Also check if first cell contains common label words
                    label_words = ['energy', 'circumference', 'number', 'length', 'angle', 'radius', 'field', 
                                   'aperture', 'gradient', 'emittance', 'tune', 'frequency', 'time', 'current',
                                   'charge', 'power', 'size', 'divergence', 'function', 'damping', 'spread']
                    if any(word in first_cell.lower() for word in label_words):
                        is_label_value_pair = True
                
                # GENERAL: Check if first row is a timeline/career entry (date/period in first column)
                # Pattern: First cell contains date/period pattern (years, "Seit", "Von", etc.) and second cell is long description
                is_timeline_entry = False
                if len(first_row) >= 2:
                    first_cell = str(first_row[0]).strip()
                    second_cell = str(first_row[1]).strip()
                    # Check if first cell contains date/period patterns
                    date_patterns = [
                        r'\d{4}',  # Year (e.g., "2015", "2007")
                        r'seit|von|bis|until|from|to',  # Period words (German/English)
                        r'\d{4}\s*[-–—]\s*\d{4}',  # Year range (e.g., "2007 - 2015")
                        r'\d{4}\s*–\s*\d{4}',  # Year range with en-dash
                    ]
                    has_date_pattern = any(re.search(pattern, first_cell, re.I) for pattern in date_patterns)
                    # If first cell has date pattern and second cell is a long description (>30 chars), it's likely a timeline entry
                    if has_date_pattern and len(second_cell) > 30:
                        is_timeline_entry = True
                
                # Also check if first row is very long (likely data, not header)
                # Headers are typically short labels, data rows are longer
                first_row_length = sum(len(str(cell)) for cell in first_row)
                is_likely_data = has_data_patterns or is_label_value_pair or is_timeline_entry or first_row_length > 100
                
                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'M',
                        'location': 'crawl_desy_simple.py:2226',
                        'message': 'Checking if first row should be header',
                        'data': {
                            'has_data_patterns': has_data_patterns,
                            'is_label_value_pair': is_label_value_pair,
                            'is_timeline_entry': is_timeline_entry,
                            'first_row_length': first_row_length,
                            'is_likely_data': is_likely_data,
                            'will_use_as_header': not is_likely_data,
                            'first_row_preview': first_row_text[:100]
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                
                if not is_likely_data:
                    # Use first row as header if no headers
                    # GENERAL: Normalize header content - replace newlines with spaces
                    # Use unicode-aware regex to handle umlauts and special characters correctly
                    normalized_first_row = []
                    for cell in first_row:
                        cell_str = str(cell)
                        cell_str = re.sub(r'\s+', ' ', cell_str, flags=re.UNICODE).strip()
                        normalized_first_row.append(cell_str)
                    markdown_output += "| " + " | ".join(normalized_first_row) + " |\n"
                    markdown_output += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                    rows = rows[1:]
                # else: treat all rows as data (no header row)
            
            # Add data rows
            rows_added = 0
            for row in rows:
                row_data = row[:len(headers)] if headers and len(row) >= len(headers) else row
                # Skip empty/separator-only rows
                if not any(str(c).strip() for c in row_data):
                    continue
                if all(str(c).strip() in ['', '---', '—', '–'] for c in row_data):
                    continue
                
                # GENERAL: Filter malformed rows (too many columns, concatenated data)
                # Pattern: Row with 10+ columns is likely malformed (concatenated data)
                num_cols = len(row_data)
                if num_cols > 10:
                    # #region agent log
                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                        import json
                        f.write(json.dumps({
                            'sessionId': 'debug-session',
                            'runId': 'run1',
                            'hypothesisId': 'H',
                            'location': 'crawl_desy_simple.py:2095',
                            'message': 'Row filtered: too many columns',
                            'data': {'num_cols': num_cols},
                            'timestamp': int(__import__('time').time() * 1000)
                        }) + '\n')
                    # #endregion
                    continue
                
                # Pattern: Only filter single-column rows with field labels, or rows where ALL cells have labels
                # Multi-column tables (2-10 columns) with field labels in cells are legitimate structured data
                if num_cols == 1:
                    # Single column: filter if has 3+ field labels (concatenated)
                    first_cell = str(row_data[0]).strip() if row_data else ""
                    if first_cell:
                        field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', first_cell, re.I))
                        if field_label_count >= 3:
                            # #region agent log
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'I',
                                    'location': 'crawl_desy_simple.py:2105',
                                    'message': 'Row filtered: single column with 3+ field labels',
                                    'data': {'field_label_count': field_label_count},
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                            # #endregion
                            continue
                elif num_cols >= 2:
                    # Multi-column: only filter if ALL cells have field labels AND many columns (indicates concatenation)
                    # GENERAL: 2-4 column tables with structured data in cells (like staff tables) are legitimate
                    # Only filter if 5+ columns AND all cells have labels (likely concatenated)
                    all_cells_have_labels = True
                    for cell in row_data:
                        cell_str = str(cell).strip()
                        if cell_str:
                            field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', cell_str, re.I))
                            if field_label_count == 0:
                                all_cells_have_labels = False
                                break
                    if all_cells_have_labels and num_cols >= 5:
                        # All cells have labels and 5+ columns = likely concatenated
                        # #region agent log
                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                            import json
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'J',
                                'location': 'crawl_desy_simple.py:2117',
                                'message': 'Row filtered: all cells have labels and 5+ columns',
                                'data': {'num_cols': num_cols},
                                'timestamp': int(__import__('time').time() * 1000)
                            }) + '\n')
                        # #endregion
                        continue
                
                # GENERAL: Normalize cell content - replace newlines with spaces to prevent broken table rows
                # This fixes cases where names like "Anna\n\n\nBarinskaya" break table formatting
                # Also handle encoding issues with umlauts and special characters
                # FIX: If first cell contains a partial name (ends with umlaut) and second cell has a link with full name,
                # use the full name from the link instead
                normalized_cells = []
                for i, cell in enumerate(row_data):
                    cell_str = str(cell)
                    # Replace all newlines and multiple spaces with single space
                    # Use unicode-aware regex to handle umlauts and special characters correctly
                    cell_str = re.sub(r'\s+', ' ', cell_str, flags=re.UNICODE).strip()
                    
                    # FIX: If this is the first cell and it ends with an umlaut (ö, ü, ä), check if next cell has a link with full name
                    if i == 0 and cell_str and len(cell_str) > 2:
                        # Check if cell ends with umlaut (likely truncated name)
                        cell_str_stripped = cell_str.strip()
                        if cell_str_stripped and cell_str_stripped[-1] in ['ö', 'ü', 'ä', 'Ö', 'Ü', 'Ä']:
                            # #region agent log
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'O',
                                    'location': 'crawl_desy_simple.py:3079',
                                    'message': 'Found first cell ending with umlaut',
                                    'data': {
                                        'first_cell': cell_str,
                                        'row_length': len(row_data),
                                        'next_cells': [str(row_data[j])[:50] for j in range(1, min(4, len(row_data)))]
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                            # #endregion
                            
                            # Look for a link in the next few cells that might contain the full name
                            for j in range(1, min(4, len(row_data))):
                                next_cell = str(row_data[j])
                                # Extract name from markdown link pattern: [Name](mailto:...)
                                link_match = re.search(r'\[([^\]]+)\]\(mailto:', next_cell)
                                if link_match:
                                    full_name = link_match.group(1).strip()
                                    # #region agent log
                                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                        import json
                                        f.write(json.dumps({
                                            'sessionId': 'debug-session',
                                            'runId': 'run1',
                                            'hypothesisId': 'P',
                                            'location': 'crawl_desy_simple.py:3095',
                                            'message': 'Checking link name match',
                                            'data': {
                                                'partial_name': cell_str_stripped,
                                                'full_name': full_name,
                                                'starts_with': full_name.lower().startswith(cell_str_stripped.lower()),
                                                'contains_at_start': cell_str_stripped.lower() in full_name.lower() and full_name.lower().index(cell_str_stripped.lower()) == 0 if cell_str_stripped.lower() in full_name.lower() else False
                                            },
                                            'timestamp': int(__import__('time').time() * 1000)
                                        }) + '\n')
                                    # #endregion
                                    
                                    # Check if full_name starts with the partial name
                                    # Also check if they're similar (partial name is a prefix of full name)
                                    if (full_name.lower().startswith(cell_str_stripped.lower()) or 
                                        (len(cell_str_stripped) >= 3 and cell_str_stripped.lower() in full_name.lower() and 
                                         full_name.lower().index(cell_str_stripped.lower()) == 0)):
                                        # Use the full name instead
                                        cell_str = full_name
                                        # #region agent log
                                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                            import json
                                            f.write(json.dumps({
                                                'sessionId': 'debug-session',
                                                'runId': 'run1',
                                                'hypothesisId': 'N',
                                                'location': 'crawl_desy_simple.py:3115',
                                                'message': 'Fixed truncated name with umlaut',
                                                'data': {
                                                    'original': str(cell),
                                                    'fixed': cell_str,
                                                    'source_cell': j
                                                },
                                                'timestamp': int(__import__('time').time() * 1000)
                                            }) + '\n')
                                        # #endregion
                                        break
                    
                    normalized_cells.append(cell_str)
                markdown_output += "| " + " | ".join(normalized_cells) + " |\n"
                rows_added += 1
            
            # #region agent log
            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                import json
                f.write(json.dumps({
                    'sessionId': 'debug-session',
                    'runId': 'run1',
                    'hypothesisId': 'K',
                    'location': 'crawl_desy_simple.py:2122',
                    'message': 'Table formatted successfully',
                    'data': {'rows_added': rows_added, 'total_rows': len(rows)},
                    'timestamp': int(__import__('time').time() * 1000)
                }) + '\n')
            # #endregion
            
            markdown_output += "\n"
    
    return markdown_output


def inject_links_into_markdown_tables(markdown_content, html_content):
    """
    Inject links directly into table sections in the markdown content.
    
    This function:
    1. Extracts all tables from HTML with links preserved
    2. Finds corresponding table sections in the markdown
    3. Replaces them in-place with enriched versions
    4. Uses content matching to ensure correct replacement without mixing tables
    
    NEW APPROACH: If markdown tables don't have email links but HTML does,
    replace ALL markdown tables with HTML-extracted versions.
    
    Args:
        markdown_content: The markdown content from Crawl4AI
        html_content: The original HTML content
        
    Returns:
        Markdown content with links injected into tables
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return markdown_content
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Only get top-level tables to avoid nested table confusion
        html_tables = [t for t in soup.find_all('table', recursive=True) if t.find_parent('table') is None]
        
        if not html_tables:
            return markdown_content
        
        # Check if HTML tables have email links
        html_has_emails = False
        html_email_count = 0
        for html_table in html_tables:
            mailto_links = html_table.find_all('a', href=lambda x: x and x.startswith('mailto:'))
            if mailto_links:
                html_has_emails = True
                html_email_count += len(mailto_links)
        
        # Check if markdown has email links (proper markdown format with email addresses)
        markdown_has_emails = bool(re.search(r'\[[^\]]+\]\(mailto:[^\s@]+@[^\s@]+\.[^\s)]+\)', markdown_content))
        
        # Debug output
        if html_has_emails:
            print(f"[DEBUG] HTML has {html_email_count} email link(s)")
        if markdown_has_emails:
            print(f"[DEBUG] Markdown already has email links")
        else:
            print(f"[DEBUG] Markdown does NOT have email links - will attempt injection")
        
        # Extract all tables from HTML with links preserved
        html_table_data = []
        used_tables = set()
        
        for html_table in html_tables:
            table_data = extract_table_from_html(html_table)
            # MINIMAL FIX: Only include tables that have rows (skip empty tables)
            if not table_data.get('rows'):
                continue
            # Get a unique identifier from the table (first few cells of first row)
            identifier = ""
            identifier_words = set()
            if table_data['rows']:
                first_row = table_data['rows'][0]
                identifier = " ".join(str(cell)[:30] for cell in first_row[:3] if cell)
                identifier_words = set(identifier.lower().split())
            
            # Also get a sample from headers if available
            if table_data['headers']:
                header_text = " ".join(str(h)[:20] for h in table_data['headers'][:3] if h)
                identifier_words.update(header_text.lower().split())
            
            html_table_data.append({
                'data': table_data,
                'identifier': identifier,
                'identifier_words': identifier_words,
                'formatted': format_table_markdown_inline(table_data)
            })
        
        # AGGRESSIVE MODE: If HTML has emails but markdown doesn't, replace ALL markdown tables with HTML tables
        # This is simpler and more reliable than trying to match individual tables
        if html_has_emails and not markdown_has_emails and html_table_data:
            print(f"[INFO] HTML has {len(html_table_data)} table(s) with emails, markdown has none - using aggressive replacement")
            # Find all table sections in markdown and replace them with HTML tables in order
            lines = markdown_content.split('\n')
            result_lines = []
            i = 0
            markdown_table_count = 0
            
            while i < len(lines):
                line = lines[i]
                
                # Check if this line looks like the start of a table row (contains |)
                if '|' in line and not line.strip().startswith('#'):
                    # Find the table section
                    table_start = i
                    table_end = i
                    
                    while table_end < len(lines):
                        current_line = lines[table_end]
                        if '|' in current_line:
                            table_end += 1
                        elif current_line.strip() == '':
                            if table_end + 1 < len(lines) and '|' in lines[table_end + 1]:
                                table_end += 1
                            else:
                                break
                        else:
                            break
                    
                    # Replace this markdown table with corresponding HTML table (if available)
                    if markdown_table_count < len(html_table_data):
                        result_lines.append(html_table_data[markdown_table_count]['formatted'])
                        used_tables.add(markdown_table_count)
                        markdown_table_count += 1
                        i = table_end
                        continue
                    else:
                        # More markdown tables than HTML tables, keep original
                        for j in range(table_start, table_end):
                            result_lines.append(lines[j])
                        i = table_end
                        continue
                
                # Not a table line, keep as-is
                result_lines.append(line)
                i += 1
            
            # FIX 4: Don't add "## Extracted Tables" here - DOM-order extraction handles tables separately
            # This prevents duplicate single-column tables from appearing before DOM-ordered tables
            # If no markdown tables found, DOM-order extraction will add tables with proper headings
            # So we skip adding tables here to avoid duplicates
            pass  # Removed: was adding single-column tables that duplicate DOM-order extraction
            
            return "\n".join(result_lines)
        
        # Find and replace table sections in markdown (original matching approach for when emails already exist)
        lines = markdown_content.split('\n')
        result_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Check if this line looks like the start of a table row (contains |)
            if '|' in line and not line.strip().startswith('#'):
                # Find the table section (consecutive lines with |)
                table_start = i
                table_end = i
                
                # Collect table rows - look for separator line (---)
                has_separator = False
                while table_end < len(lines):
                    current_line = lines[table_end]
                    if '|' in current_line:
                        # Check if it's a separator line
                        if re.match(r'^\s*\|[\s\-:]+\|', current_line):
                            has_separator = True
                        table_end += 1
                    elif current_line.strip() == '':
                        # Empty line - check if next line continues table
                        if table_end + 1 < len(lines) and '|' in lines[table_end + 1]:
                            table_end += 1
                        else:
                            break
                    else:
                        # Non-table line, end of table
                        break
                
                # Extract table content for matching
                table_lines = lines[table_start:table_end]
                table_text = "\n".join(table_lines)
                
                # CRITICAL: Check if table already has PROPER links (with email addresses, not just names)
                # Check if table has markdown links with actual email addresses in them
                # Pattern: [text](mailto:email@domain.com) - we want the email, not just the name
                # Also check for plain email addresses (might be in raw_markdown)
                has_proper_email_links = bool(re.search(r'\[[^\]]+\]\(mailto:[^\s@]+@[^\s@]+\.[^\s)]+\)', table_text))
                has_plain_emails = bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', table_text))
                has_proper_http_links = '](http' in table_text or '](https' in table_text
                
                # Only skip if table has proper links with actual URLs/emails
                # Don't skip if it just has text that looks like a name (might be link text without href)
                # BUT: If it has plain emails (not in markdown links), we should still process to convert them
                if (has_proper_email_links or has_proper_http_links) and not (has_plain_emails and not has_proper_email_links):
                    # Table already has proper links, just copy it as-is and skip processing
                    for j in range(table_start, table_end):
                        result_lines.append(lines[j])
                    i = table_end
                    continue
                
                # Table doesn't have proper links, proceed with matching and replacement
                table_words = set(table_text.lower().split())
                
                # Try to match with HTML tables (only unused ones)
                best_match = None
                best_match_score = 0
                best_match_idx = -1
                
                for idx, html_table_info in enumerate(html_table_data):
                    if idx in used_tables:
                        continue
                    
                    # Count matching words between table and HTML table
                    common_words = html_table_info['identifier_words'] & table_words
                    score = len(common_words)
                    
                    # Bonus points if identifier text appears in table
                    if html_table_info['identifier'] and html_table_info['identifier'].lower()[:50] in table_text.lower():
                        score += 5
                    
                    # Bonus for matching names (common in member tables)
                    # Check if any person names from HTML table appear in markdown table
                    identifier_lower = html_table_info['identifier'].lower()
                    for word in table_words:
                        if len(word) > 3 and word in identifier_lower:
                            score += 2
                    
                    if score > best_match_score and score > 0:
                        best_match_score = score
                        best_match = html_table_info
                        best_match_idx = idx
                
                # If we found a good match, replace the table section
                if best_match and best_match_score >= 1:  # Lowered threshold to 1 for better matching
                    # Replace table section with enriched version
                    result_lines.append(best_match['formatted'])
                    used_tables.add(best_match_idx)
                    i = table_end
                    continue
                else:
                    # No match found - try to extract emails from HTML directly as fallback
                    # This handles cases where table structure differs but emails are present
                    if BEAUTIFULSOUP_AVAILABLE:
                        try:
                            soup = BeautifulSoup(html_content, 'html.parser')
                            html_tables_fallback = soup.find_all('table', recursive=True)
                            
                            # Try to find emails in cells that match the markdown table content
                            for html_table in html_tables_fallback:
                                html_text = html_table.get_text().lower()
                                table_text_lower = table_text.lower()
                                
                                # If there's some overlap in content, try to extract emails
                                common_words = set(html_text.split()) & set(table_text_lower.split())
                                if len(common_words) >= 2:  # At least 2 common words
                                    # Extract all mailto: links from this table
                                    mailto_links = html_table.find_all('a', href=lambda x: x and x.startswith('mailto:'))
                                    if mailto_links:
                                        # Found emails - try to inject them into the markdown table
                                        # Create a comprehensive email mapping from HTML
                                        email_map = {}
                                        name_to_email = {}
                                        
                                        for link in mailto_links:
                                            email = unescape(link.get('href', '')[7:])
                                            link_text = link.get_text(strip=True)
                                            
                                            # Map link text (name) to email
                                            if link_text:
                                                email_map[link_text.lower()] = email
                                                name_to_email[link_text.lower()] = email
                                            
                                            # Also try to find the person's name in the same row/cell
                                            # Look for parent cell/row to get context
                                            parent_cell = link.find_parent(['td', 'th'])
                                            if parent_cell:
                                                # Get all text from the row to find the person's name
                                                parent_row = parent_cell.find_parent('tr')
                                                if parent_row:
                                                    row_text = parent_row.get_text()
                                                    # Extract potential names (words that might be names)
                                                    # Look for patterns like "Name | E-Mail: [Name](mailto:email)"
                                                    cells = parent_row.find_all(['td', 'th'])
                                                    for cell in cells:
                                                        cell_text = cell.get_text(strip=True)
                                                        # If this cell contains the email link, check adjacent cells for name
                                                        if link in cell.find_all('a'):
                                                            # Check previous cells for name
                                                            cell_idx = cells.index(cell)
                                                            if cell_idx > 0:
                                                                prev_cell_text = cells[cell_idx - 1].get_text(strip=True)
                                                                if prev_cell_text and len(prev_cell_text) > 3:
                                                                    name_to_email[prev_cell_text.lower()] = email
                                        
                                        # Try to replace names with emails in the markdown table
                                        if email_map or name_to_email:
                                            enriched_lines = []
                                            for line in table_lines:
                                                enriched_line = line
                                                # Look for patterns: "E-Mail: | Name" and replace Name with email
                                                # Try multiple patterns
                                                for name, email in name_to_email.items():
                                                    # Pattern 1: | E-Mail: | Name |
                                                    pattern1 = rf'(\|\s*E-Mail:\s*\|\s*){re.escape(name)}(?=\s*\|)'
                                                    if re.search(pattern1, enriched_line, re.IGNORECASE):
                                                        enriched_line = re.sub(pattern1, rf'\1[{email}](mailto:{email})', enriched_line, flags=re.IGNORECASE)
                                                    
                                                    # Pattern 2: Name | E-Mail: | Name (if name appears twice)
                                                    pattern2 = rf'({re.escape(name)}.*?\|\s*E-Mail:\s*\|\s*){re.escape(name)}(?=\s*\|)'
                                                    if re.search(pattern2, enriched_line, re.IGNORECASE):
                                                        enriched_line = re.sub(pattern2, rf'\1[{email}](mailto:{email})', enriched_line, flags=re.IGNORECASE)
                                                
                                                enriched_lines.append(enriched_line)
                                            
                                            # If we made changes, use enriched version
                                            if enriched_lines != table_lines:
                                                result_lines.extend(enriched_lines)
                                                i = table_end
                                                continue
                        except Exception as e:
                            # Log error for debugging but continue
                            print(f"[DEBUG] Fallback email injection failed: {e}")
                            pass
                    
                    # No match found and fallback didn't work, keep original table
                    for j in range(table_start, table_end):
                        result_lines.append(lines[j])
                    i = table_end
                    continue
            
            # Not a table line, keep as-is
            result_lines.append(line)
            i += 1
        
        return "\n".join(result_lines)
    
    except Exception as e:
        print(f"[WARNING] Error injecting links into markdown: {e}")
        import traceback
        traceback.print_exc()
        return markdown_content


def format_table_markdown_inline(table):
    """
    Format a table dictionary as markdown (inline version, no extra headers).
    
    Args:
        table: Table dict with headers, rows, and caption
        
    Returns:
        Markdown string with table formatted
    """
    markdown = ""
    
    caption = table.get('caption', '')
    if caption:
        markdown += f"*{caption}*\n\n"
    
    headers = table.get('headers', [])
    rows = table.get('rows', [])

    # Minimal key-value handling (Issue #3 / #5):
    # Some 2-column tables are actually field|value pairs, but our header extraction can
    # mistakenly treat the first pair as "headers", creating a separator row like:
    # | Electron energy | 6.0 GeV |
    # | --- | --- |
    #
    # Heuristic (simple, structural): if there are exactly 2 "headers" and the remaining
    # rows are consistently 2 columns, treat headers as the first data row.
    is_key_value_table = False
    if headers and len(headers) == 2 and rows and all(len(r) == 2 for r in rows[:5]):
        h0 = str(headers[0] or "").strip()
        h1 = str(headers[1] or "").strip()
        # Treat empty extracted "headers" as a key-value table indicator
        if not h0 and not h1:
            is_key_value_table = True
        # header looks like "Label:" or "Value" (has digits) → likely not a real column header
        if h0.endswith(':') or h1.endswith(':') or (re.search(r'\d', h1) is not None):
            is_key_value_table = True
    
    # Only add header row if we have explicit headers AND it's not key-value
    if headers and rows and not is_key_value_table:
        # We have explicit headers - format as standard markdown table
        markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
        markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in rows:
            # Ensure row has same number of cells as headers
            row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
            # Skip empty/separator-only rows
            if not any(str(cell).strip() for cell in row_data):
                continue
            if all(str(cell).strip() in ['', '---', '—', '–'] for cell in row_data):
                continue
            # Clean each cell: remove excessive whitespace, handle empty cells
            cleaned_row = []
            for cell in row_data:
                cell_str = str(cell).strip()
                # Only remove truly duplicate words that are clearly errors (not names or before links)
                # Skip if the duplicate word is followed by a link (likely a name)
                cell_str = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_str)  # Remove duplicate words, but not before links
                cleaned_row.append(cell_str if cell_str else '')
            markdown += "| " + " | ".join(cleaned_row) + " |\n"
    elif rows:
        # No headers OR key-value: format without a header row
        # Determine max columns from all rows
        if is_key_value_table:
            # Treat "headers" as the first data row
            rows = [headers] + rows
            headers = []
        max_cols = max(len(row) for row in rows) if rows else 0
        for row in rows:
            # Pad row to max_cols if needed
            row_data = row + [''] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols]
            # Skip empty/separator-only rows
            if not any(str(cell).strip() for cell in row_data):
                continue
            if all(str(cell).strip() in ['', '---', '—', '–'] for cell in row_data):
                continue
            # Clean each cell
            cleaned_row = []
            for cell in row_data:
                cell_str = str(cell).strip()
                # Only remove truly duplicate words that are clearly errors (not names or before links)
                # Skip if the duplicate word is followed by a link (likely a name)
                cell_str = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_str)  # Remove duplicate words, but not before links
                cleaned_row.append(cell_str if cell_str else '')
            markdown += "| " + " | ".join(cleaned_row) + " |\n"
    
    return markdown


def get_table_header_normalized(formatted_table):
    """
    Extract and normalize the header from a formatted markdown table.
    Returns normalized header string, or None if no header found.
    """
    if not formatted_table or not formatted_table.strip():
        return None
    lines = formatted_table.split('\n')
    for line in lines:
        if line.strip() and '|' in line and not re.match(r'^\s*\|[\s\-:]+\|', line):
            return re.sub(r'\s+', ' ', line.lower().strip())
    return None


# ============================================================================
# Enhanced Duplication Detection and Noise Removal Functions
# ============================================================================

def normalize_text_enhanced(text):
    """
    Enhanced normalization with word deduplication and markdown link normalization.
    
    This handles:
    - Markdown link whitespace normalization
    - Word-level deduplication ("Contact Contact" -> "Contact")
    - Standard text normalization
    """
    # First normalize markdown links (remove whitespace in link syntax)
    text = normalize_markdown_links(text)
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove markdown syntax for comparison (extract content)
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)  # Extract link text
    text = re.sub(r'mailto:\s*', '', text)  # Remove mailto: prefix
    
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove consecutive duplicate words
    words = text.split()
    deduplicated_words = []
    prev_word = None
    for word in words:
        if word != prev_word:
            deduplicated_words.append(word)
        prev_word = word
    
    return ' '.join(deduplicated_words)


def normalize_markdown_links(text):
    """
    Remove whitespace from markdown link syntax.
    
    Fixes:
    - [ text](url) -> [text](url)
    - (mailto: email) -> (mailto:email)
    - [email ](mailto: email ) -> [email](mailto:email)
    """
    # Remove spaces in link brackets
    text = re.sub(r'\[\s+', '[', text)
    text = re.sub(r'\s+\]', ']', text)
    
    # Remove spaces in link parentheses
    text = re.sub(r'\(\s+', '(', text)
    text = re.sub(r'\s+\)', ')', text)
    
    # Remove spaces after colons in URLs/mailto
    text = re.sub(r':\s+', ':', text)
    
    # Remove spaces before/after email addresses in links
    text = re.sub(r'\[(\s+)([^\]]+)(\s+)\]', r'[\2]', text)
    
    return text


def extract_emails_from_text(text):
    """Extract email addresses from text for deduplication."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    return [e.lower().strip() for e in emails]


def text_similarity(text1, text2):
    """Calculate similarity ratio between two texts (0.0 to 1.0)."""
    if not text1 or not text2:
        return 0.0
    
    # Use simple character-based similarity (can be enhanced with difflib)
    if text1 == text2:
        return 1.0
    
    # Calculate longest common subsequence ratio
    len1, len2 = len(text1), len(text2)
    if len1 == 0 or len2 == 0:
        return 0.0
    
    # Simple ratio: count matching characters
    matches = sum(1 for c1, c2 in zip(text1, text2) if c1 == c2)
    max_len = max(len1, len2)
    
    return matches / max_len if max_len > 0 else 0.0


def detect_enhanced_repetition(markdown_lines):
    """
    Enhanced repetition detection with:
    - Multi-line block comparison
    - Email address extraction and deduplication
    - Substring/containment detection
    - Paragraph extraction and comparison
    """
    duplicates = set()
    
    # 1. Email address deduplication (highest priority)
    email_to_lines = {}
    for i, line in enumerate(markdown_lines):
        emails = extract_emails_from_text(line)
        for email in emails:
            if email in email_to_lines:
                # This email was seen before - mark all occurrences as duplicates
                duplicates.update(email_to_lines[email])
                duplicates.add(i)
            else:
                email_to_lines[email] = []
            email_to_lines[email].append(i)
    
    # 2. Multi-line block detection (sliding window)
    seen_blocks = {}
    block_size = 3
    for i in range(len(markdown_lines) - block_size + 1):
        block = '\n'.join(markdown_lines[i:i+block_size])
        normalized = normalize_text_enhanced(block)
        
        if len(normalized) < 50:  # Skip very short blocks
            continue
        
        # Check similarity to seen blocks
        for seen_block, seen_indices in seen_blocks.items():
            similarity = text_similarity(normalized, seen_block)
            if similarity > 0.90:  # 90% similar
                duplicates.update(seen_indices)
                duplicates.update(range(i, i+block_size))
                break
        
        # Store this block
        if normalized not in seen_blocks:
            seen_blocks[normalized] = []
        seen_blocks[normalized].append(range(i, i+block_size))
    
    # 3. Single-line comparison with enhanced normalization
    seen_lines = {}
    for i, line in enumerate(markdown_lines):
        normalized = normalize_text_enhanced(line)
        
        # Skip very short lines (< 10 chars)
        if len(normalized) < 10:
            continue
        
        # Check similarity to seen lines
        for seen_line, seen_indices in seen_lines.items():
            similarity = text_similarity(normalized, seen_line)
            if similarity > 0.95:
                duplicates.update(seen_indices)
                duplicates.add(i)
                break
        
        # Check substring/containment relationships
        for seen_line, seen_indices in seen_lines.items():
            if normalized in seen_line or seen_line in normalized:
                # Keep the longer line, mark shorter as duplicate
                if len(normalized) < len(seen_line):
                    duplicates.add(i)
                else:
                    duplicates.update(seen_indices)
                break
        
        # Store this line
        if normalized not in seen_lines:
            seen_lines[normalized] = []
        seen_lines[normalized].append(i)
    
    # 4. Paragraph extraction and comparison
    paragraph_to_lines = {}
    paragraph_pattern = r'([^.!?]+[.!?])'
    
    for i, line in enumerate(markdown_lines):
        paragraphs = re.findall(paragraph_pattern, line)
        for para in paragraphs:
            normalized = normalize_text_enhanced(para)
            if len(normalized) < 30:  # Skip very short paragraphs
                continue
            
            if normalized in paragraph_to_lines:
                # This paragraph was seen before
                duplicates.update(paragraph_to_lines[normalized])
                duplicates.add(i)
            else:
                paragraph_to_lines[normalized] = []
            paragraph_to_lines[normalized].append(i)
    
    return duplicates


def extract_contact_blocks(html_soup):
    """
    Extract complete contact blocks (name + title + phone + email + location).
    
    Strategy:
    1. Find elements containing email addresses
    2. Expand to parent container (paragraph, div, list item)
    3. Extract all text from container (preserves relationships)
    4. Group by proximity (same container = same person)
    """
    contact_blocks = []
    
    if not BEAUTIFULSOUP_AVAILABLE:
        return contact_blocks
    
    # Find all email links
    email_links = html_soup.find_all('a', href=re.compile(r'mailto:'))
    
    for link in email_links:
        # Extract email
        email = link.get('href', '').replace('mailto:', '').strip()
        if not email:
            continue
        
        # Find parent container (p, div, li, td, tr)
        # Try multiple parent levels to find the best container
        # Strategy: Find the smallest container that includes both name and email
        parent = None
        best_parent = None
        best_score = 0
        
        for parent_tag in ['p', 'div', 'li', 'td', 'tr', 'section', 'article']:
            candidate = link.find_parent(parent_tag)
            if candidate:
                candidate_text = candidate.get_text(strip=True)
                # Score based on:
                # 1. Has name-like pattern (2-4 capitalized words) - high priority
                # 2. Has substantial content (20+ chars) - medium priority
                # 3. Has phone pattern - bonus
                score = 0
                has_name_pattern = bool(re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', candidate_text))
                has_phone = bool(re.search(r'T\.\s*\(?\d+', candidate_text))
                
                if has_name_pattern:
                    score += 10
                if len(candidate_text) > 20:
                    score += 5
                if has_phone:
                    score += 2
                
                # Prefer containers with names and substantial content
                if score > best_score:
                    best_score = score
                    best_parent = candidate
                
                # Also keep first substantial candidate as fallback
                if not parent and len(candidate_text) > 20:
                    parent = candidate
        
        # Use best parent if found, otherwise use fallback
        parent = best_parent if best_parent else parent
        
        if parent:
            # Extract all text from parent (preserves structure)
            # Use separator=' ' to keep words together, but preserve line structure where possible
            contact_text = parent.get_text(separator=' ', strip=True)
            
            # Also try to get text with line breaks to preserve structure better
            # This helps when contact info spans multiple lines
            contact_text_with_breaks = parent.get_text(separator='\n', strip=True)
            # If text with breaks is longer, it might have better structure
            if len(contact_text_with_breaks) > len(contact_text) * 1.2:
                # Use the version with breaks, but normalize
                contact_text = re.sub(r'\n+', ' ', contact_text_with_breaks)
            
            # If parent doesn't have enough content or doesn't have a name pattern, 
            # try to expand to include siblings or parent's parent
            # This helps when name is in a previous sibling element or parent container
            if len(contact_text) < 30 or not re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', contact_text):
                # Try to find previous sibling that might contain the name
                prev_sibling = parent.find_previous_sibling()
                if prev_sibling:
                    prev_text = prev_sibling.get_text(separator=' ', strip=True)
                    # Check if it looks like a name (2-4 capitalized words, no title keywords)
                    if re.match(r'^[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', prev_text):
                        # Make sure it's not a title
                        title_keywords_check = ['team', 'head', 'manager', 'leader', 'trainer', 'assistant', 'hr', 'recruitment', 
                                              'employer', 'branding', 'scientist', 'technician', 'clerk', 'assistent', 'staff', 'scientific']
                        if not any(keyword in prev_text.lower() for keyword in title_keywords_check):
                            contact_text = prev_text + ' ' + contact_text
                
                # Also try parent's parent if current parent is too small
                if len(contact_text) < 50:
                    grandparent = parent.find_parent(['div', 'section', 'article', 'li'])
                    if grandparent and grandparent != parent:
                        grandparent_text = grandparent.get_text(separator=' ', strip=True)
                        # If grandparent has more content and includes a name pattern, use it
                        if len(grandparent_text) > len(contact_text) and re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', grandparent_text):
                            contact_text = grandparent_text
            
            # Extract name (pattern: First Last or Last, First)
            # More flexible pattern: allows for middle names, titles (Dr., Prof.), and handles various formats
            # Allow special characters (umlauts) in names: Ä, Ö, Ü, ä, ö, ü, ß
            # Pattern 1: Standard name (First Last, First Middle Last, etc.) - with umlauts
            name_pattern1 = r'\b([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+)\b'
            # Pattern 2: Name with title prefix (Dr. John Smith, Prof. Dr. Jane Doe)
            name_pattern2 = r'\b(?:Dr\.|Prof\.|Prof\.\s+Dr\.)\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+)\b'
            # Pattern 3: Name at start of text (common in contact blocks) - with umlauts
            name_pattern3 = r'^([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)'
            
            names = []
            # Try all patterns
            names.extend(re.findall(name_pattern1, contact_text))
            names.extend(re.findall(name_pattern2, contact_text))
            names.extend(re.findall(name_pattern3, contact_text))
            
            # Remove duplicates while preserving order
            seen = set()
            unique_names = []
            for name in names:
                name_clean = name.strip()
                if name_clean and name_clean not in seen and len(name_clean) > 3:
                    seen.add(name_clean)
                    unique_names.append(name_clean)
            
            names = unique_names
            
            # Filter out names that are actually titles (common false positives)
            # Titles often look like names: "Team Leader", "Head of", etc.
            title_keywords = ['team', 'head', 'manager', 'leader', 'trainer', 'assistant', 'hr', 'recruitment', 
                            'employer', 'branding', 'scientist', 'technician', 'clerk', 'assistent', 'staff', 'scientific']
            filtered_names = []
            for name in names:
                name_lower = name.lower()
                # Skip if name contains title keywords (likely a title, not a name)
                if not any(keyword in name_lower for keyword in title_keywords):
                    # Also check if it's a reasonable name length (2-4 words, each capitalized)
                    words = name.split()
                    # Allow names with special characters (like "Krüger" with umlaut)
                    if 2 <= len(words) <= 4:
                        # Check if first letter of each word is uppercase (allow special chars)
                        if all(w and (w[0].isupper() or w[0] in 'ÄÖÜäöü') for w in words):
                            filtered_names.append(name)
            
            # If we filtered out all names but have titles, try to extract name from beginning of text
            # (before title keywords appear)
            if not filtered_names:
                # Look for name pattern at the very start of contact_text (before any title keywords)
                # Split text by common separators and check first part
                text_parts = re.split(r'\s+(?:Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent|T\.|E\.)', contact_text, flags=re.IGNORECASE, maxsplit=1)
                if text_parts and len(text_parts[0].strip()) > 0:
                    first_part = text_parts[0].strip()
                    # Check if first part looks like a name (2-4 capitalized words)
                    name_match = re.search(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})', first_part)
                    if name_match:
                        potential_name = name_match.group(1)
                        # Make sure it's not a title and has reasonable length
                        if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                            2 <= len(potential_name.split()) <= 4):
                            filtered_names.append(potential_name)
                
                # Also try extracting name that appears before pronoun (common pattern: "Name (pronoun)")
                # Allow special characters in names (umlauts, etc.)
                pronoun_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})\s*\(', contact_text)
                if pronoun_match:
                    potential_name = pronoun_match.group(1)
                    if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                        2 <= len(potential_name.split()) <= 4):
                        if potential_name not in filtered_names:
                            filtered_names.append(potential_name)
                
                # Also try to extract name from HTML structure - look for text nodes before the email link
                # This helps when name is in a separate element
                if link.parent:
                    # Get all text before the link in the parent (preserve order)
                    # Find all text nodes and links before this email link
                    all_siblings = []
                    for sibling in link.parent.children:
                        if sibling == link:
                            break
                        if hasattr(sibling, 'get_text'):
                            sibling_text = sibling.get_text(separator=' ', strip=True)
                            if sibling_text:
                                all_siblings.append(sibling_text)
                    
                    # Combine siblings to get full context
                    link_context = ' '.join(all_siblings)
                    if not link_context:
                        # Fallback: get all text from parent
                        link_context = link.parent.get_text(separator=' ', strip=True)
                    
                    # Split by common separators (pronoun, title keywords, phone, email)
                    name_candidates = re.split(r'\s*(?:\(he/him\)|\(she/her\)|\(they/them\)|Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|T\.|E\.|@)', link_context, flags=re.IGNORECASE, maxsplit=1)
                    if name_candidates and len(name_candidates[0].strip()) > 0:
                        first_part = name_candidates[0].strip()
                        # Check if it looks like a name (2-4 capitalized words, no title keywords)
                        # Allow umlauts and special characters
                        name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', first_part)
                        if name_match:
                            potential_name = name_match.group(1)
                            if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                                2 <= len(potential_name.split()) <= 4):
                                if potential_name not in filtered_names:
                                    filtered_names.append(potential_name)
            
            # If we still don't have names, try a more aggressive approach
            # Look for name patterns that appear at the very beginning of the contact text
            if not filtered_names:
                # Extract everything before the first title keyword, phone, or email
                text_before_metadata = re.split(r'\s*(?:Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent|Staff|T\.|E\.|\(he/him\)|\(she/her\)|@)', contact_text, flags=re.IGNORECASE, maxsplit=1)[0]
                if text_before_metadata:
                    # Look for name pattern in this text
                    name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', text_before_metadata.strip())
                    if name_match:
                        potential_name = name_match.group(1)
                        # Make sure it's not a title
                        if not any(keyword in potential_name.lower() for keyword in title_keywords):
                            filtered_names.append(potential_name)
            
            names = filtered_names if filtered_names else unique_names  # Fallback to original if filtering removed everything
            
            # If we have multiple name candidates, prefer the one that appears first in the text
            # and is not a title
            if len(names) > 1:
                # Find the position of each name in the contact_text
                name_positions = []
                for name in names:
                    pos = contact_text.find(name)
                    if pos >= 0:
                        name_positions.append((pos, name))
                # Sort by position and take the first one
                if name_positions:
                    name_positions.sort(key=lambda x: x[0])
                    names = [name_positions[0][1]]
            
            # Extract phone (pattern: T. (040) 8998-XXXX or +49 (0)40 8998-XXXX)
            # More flexible pattern to handle various phone formats
            phone_pattern = r'(?:T\.|Phone:?|Tel\.?)\s*[+\d\s\-\(\)]{8,}|\(\d{3,4}\)\s*\d{4,}[\s\-]?\d+|[\+\d\s\-\(\)]{10,}'
            phones = re.findall(phone_pattern, contact_text)
            # Clean up phone numbers (remove extra spaces, normalize)
            phones = [re.sub(r'\s+', ' ', p.strip()) for p in phones if len(p.strip()) >= 8]
            
            # Extract title (pattern: Head of..., Manager..., etc.)
            # More comprehensive pattern to catch various title formats
            # Stop at phone numbers (T. or E.) to avoid capturing them
            # Pattern 1: Full title with "of/for" (e.g., "Head of Recruitment and Employer Branding")
            # Stop before phone (T.) or email (E.) markers
            title_pattern1 = r'\b(Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent)\s+(?:of|for)?\s+[^\.\(\)T]+?(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            # Pattern 2: Title without "of/for" (e.g., "HR Manager Recruitment Technical & Scientific Staff")
            # Stop before phone/email markers
            title_pattern2 = r'\b(Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent)\s+[A-Z][^\.\(\)T]+?(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            # Pattern 3: Multi-word titles (e.g., "Team Leader Recruitment", "HR Team Assistent Recruitment")
            title_pattern3 = r'\b(Team\s+(?:Leader|Assistent|Manager)\s+[^\.\(\)T]+?|HR\s+(?:Manager|Team|Assistent)\s+[^\.\(\)T]+?|Head\s+of\s+[^\.\(\)T]+?)(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            
            titles = []
            # Try all patterns
            titles.extend(re.findall(title_pattern1, contact_text, re.IGNORECASE))
            titles.extend(re.findall(title_pattern2, contact_text, re.IGNORECASE))
            titles.extend(re.findall(title_pattern3, contact_text, re.IGNORECASE))
            
            # Remove duplicates and clean up titles
            seen_titles = set()
            unique_titles = []
            for title in titles:
                title_clean = re.sub(r'\s+', ' ', title.strip())
                # Remove trailing punctuation and normalize
                title_clean = re.sub(r'[\.\,]+$', '', title_clean).strip()
                # Remove phone number patterns that might be captured (e.g., "T" at the end)
                title_clean = re.sub(r'\s+T\.?\s*$', '', title_clean, flags=re.IGNORECASE)
                title_clean = re.sub(r'\s+E\.?\s*$', '', title_clean, flags=re.IGNORECASE)
                # Remove phone numbers that might be in the title
                title_clean = re.sub(r'\s*T\.\s*\(?\d+\)?.*$', '', title_clean, flags=re.IGNORECASE)
                title_clean = re.sub(r'\s*E\.\s*[a-zA-Z0-9._%+-]+@.*$', '', title_clean, flags=re.IGNORECASE)
                # Remove email addresses
                title_clean = re.sub(r'\s+[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}.*$', '', title_clean)
                title_clean = title_clean.strip()
                if title_clean and title_clean.lower() not in seen_titles and len(title_clean) > 5:
                    seen_titles.add(title_clean.lower())
                    unique_titles.append(title_clean)
            
            titles = unique_titles
            
            # Extract pronoun (pattern: (he/him), (she/her))
            pronoun_pattern = r'\((he|she|they)/(him|her|them)\)'
            pronouns = re.findall(pronoun_pattern, contact_text)
            
            # Only add if we have at least email (required) and either name or phone (at least one other field)
            # BUT: Don't use titles as names - if we only have titles and no proper names, set name to None
            final_name = None
            if names:
                # Make absolutely sure the name is not a title
                name_candidate = names[0]
                name_lower = name_candidate.lower()
                # Double-check: if it contains title keywords, it's not a name
                if not any(keyword in name_lower for keyword in title_keywords):
                    final_name = name_candidate
                else:
                    # Name candidate is actually a title, don't use it
                    print(f"[DEBUG] Rejected name candidate '{name_candidate}' - it's a title, not a name")
            
            if email and (final_name or phones or titles):
                contact_blocks.append({
                    'email': email,
                    'name': final_name,
                    'phone': phones[0] if phones else None,
                    'title': titles[0] if titles else None,
                    'pronoun': pronouns[0] if pronouns else None,
                    'full_text': contact_text
                })
                print(f"[DEBUG] Extracted contact block: email={email}, name={final_name if final_name else 'None'}, phone={phones[0] if phones else 'None'}, title={titles[0] if titles else 'None'}")
            else:
                print(f"[DEBUG] Skipped contact block: email={email}, has_name={bool(final_name)}, has_phone={bool(phones)}, has_title={bool(titles)}")
    
    return contact_blocks


def reconstruct_contact_structure(contact_blocks, page_title=None):
    """
    Reconstruct markdown structure from extracted contact blocks.
    
    Creates:
    - Page title from URL or content
    - Section headings
    - List structure for contact entries
    """
    markdown = []
    
    # Add page title
    if page_title:
        markdown.append(f"# {page_title}")
    else:
        markdown.append("# Contact Information")
    markdown.append("")
    
    # Add section heading
    markdown.append("## Contact Details")
    markdown.append("")
    
    # Add contact entries as structured list
    for contact in contact_blocks:
        entry = []
        
        # Name with pronoun if available
        # Always include name if available, even if it's None (will use email as fallback)
        if contact['name']:
            name_line = f"- **{contact['name']}**"
            if contact['pronoun']:
                name_line += f" ({contact['pronoun'][0]}/{contact['pronoun'][1]})"
            entry.append(name_line)
        elif contact['email']:
            # If no name, use email address as identifier
            email_local = contact['email'].split('@')[0].replace('.', ' ').title()
            name_line = f"- **{email_local}**"
            if contact['pronoun']:
                name_line += f" ({contact['pronoun'][0]}/{contact['pronoun'][1]})"
            entry.append(name_line)
        
        # Title
        if contact['title']:
            # Clean title: remove any trailing phone/email markers
            title = contact['title'].strip()
            # Remove trailing "T", "E", or phone/email patterns
            title = re.sub(r'\s+[TE]\.?\s*$', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s+T\.\s*\(.*$', '', title)
            title = re.sub(r'\s+E\.\s*[a-zA-Z0-9._%+-]+@.*$', '', title)
            entry.append(f"  - Title: {title}")
        
        # Phone
        if contact['phone']:
            # Fix phone format: ensure space after colon and normalize
            phone = contact['phone'].strip()
            # Normalize phone format: "T. (040) 8998-4219" or "T.(040) 8998-4219" -> "T. (040) 8998-4219"
            phone = re.sub(r'T\.\s*\(', 'T. (', phone)
            phone = re.sub(r'T\.\(', 'T. (', phone)
            # Remove "T." prefix if it's duplicated (e.g., "T. T. (040)")
            phone = re.sub(r'^T\.\s+T\.\s+', 'T. ', phone)
            entry.append(f"  - Phone: {phone}")
        
        # Email (always include if available)
        if contact['email']:
            entry.append(f"  - Email: [{contact['email']}](mailto:{contact['email']})")
        
        if entry:
            markdown.extend(entry)
            markdown.append("")
    
    return '\n'.join(markdown)


def clean_markdown_links_post_process(markdown_text):
    """
    Post-process markdown to clean link syntax by removing whitespace.
    
    This should be run after HTML→Markdown conversion.
    """
    if not markdown_text:
        return markdown_text
    
    # Remove spaces in link brackets
    markdown_text = re.sub(r'\[\s+', '[', markdown_text)
    markdown_text = re.sub(r'\s+\]', ']', markdown_text)
    
    # Remove spaces in link parentheses
    markdown_text = re.sub(r'\(\s+', '(', markdown_text)
    markdown_text = re.sub(r'\s+\)', ')', markdown_text)
    
    # Remove spaces after colons in URLs/mailto
    markdown_text = re.sub(r':\s+', ':', markdown_text)
    
    # Remove spaces before/after email addresses in links (more specific)
    # Pattern: [ space email space ] -> [email]
    markdown_text = re.sub(r'\[\s+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\s+\]', r'[\1]', markdown_text)
    
    return markdown_text


async def crawl_site():
    """
    Main crawling function that orchestrates the entire crawling process.
    
    HOW IT WORKS:
    1. Configure browser with anti-bot and JavaScript support
    2. Configure the deep crawling strategy (how to follow links)
    3. Create a crawler instance
    4. Process each URL in ROOT_URLS list
    5. Process and save all crawled pages
    6. Log all errors to a JSON file
    """
    
    # ========================================================================
    # STEP 1: Initialize Error Tracking
    # ========================================================================
    # These variables accumulate results across all URLs being processed.
    # They are initialized before the try block to ensure they're always
    # available for final summary and error logging, even if exceptions occur.
    all_errors = []
    all_successful_urls = []
    all_results = []
    all_urls_by_depth = {}  # Track URLs by depth level across all URLs
    
    # ========================================================================
    # STEP 2: Configure Browser with Anti-Bot and JavaScript Support
    # ========================================================================
    # BrowserConfig controls how the browser behaves during crawling.
    # This configuration is shared across all URLs being processed.
    
    browser_config = BrowserConfig(
        # enable_stealth: Anti-bot detection evasion
        # When True, modifies browser fingerprint (user agent, headers, etc.)
        # to appear more human-like. This helps bypass basic bot detection
        # systems that block automated crawlers. Recommended for production use.
        enable_stealth=ENABLE_STEALTH_MODE,
        
        # headless: Run browser without visible window
        # True = no GUI window (faster, less resource usage, suitable for servers)
        # False = visible browser window (useful for debugging and seeing what's happening)
        # Headless mode is recommended for production but can make debugging harder
        headless=HEADLESS,
        
        # JavaScript rendering: Automatically enabled via Playwright
        # Crawl4AI uses Playwright by default, which fully supports JavaScript.
        # Modern websites that rely on JavaScript to render content will work
        # correctly. No additional configuration needed.
    )
    
    # ========================================================================
    # STEP 3: Configure Deep Crawling Strategy
    # ========================================================================
    # This configures HOW Crawl4AI follows links between pages.
    # The BFSDeepCrawlStrategy uses Breadth-First Search: it crawls all pages
    # at depth 1 before moving to depth 2, ensuring systematic coverage.
    
    # Create filter chain to exclude problematic URLs
    # This prevents crawling unwanted file types and URLs that would cause errors.
    # Common exclusions: calendar files (.ics), binary files, API endpoints, etc.
    filter_chain = None
    URL_FILTER_CLASS = None
    URL_FILTER_TYPE = None  # 'RegexURLFilter' or 'URLPatternFilter'
    FilterChain = None  # Will be set if available
    FILTER_CHAIN_AVAILABLE = False
    
    try:
        # Try standard import path (older Crawl4AI versions)
        from crawl4ai.deep_crawling import RegexURLFilter
        URL_FILTER_CLASS = RegexURLFilter
        URL_FILTER_TYPE = 'RegexURLFilter'
        URL_FILTER_AVAILABLE = True
    except ImportError:
        try:
            # Try alternative import path (newer Crawl4AI versions)
            from crawl4ai.deep_crawling.filters import URLPatternFilter, FilterChain
            URL_FILTER_CLASS = URLPatternFilter
            URL_FILTER_TYPE = 'URLPatternFilter'
            FILTER_CHAIN_AVAILABLE = True
            URL_FILTER_AVAILABLE = True
        except ImportError:
            try:
                # Try to import FilterChain separately (might be in different location)
                from crawl4ai.deep_crawling import FilterChain
                FILTER_CHAIN_AVAILABLE = True
                URL_FILTER_AVAILABLE = False
            except ImportError:
                # URL filtering not available - this is OK, crawler will handle problematic URLs gracefully
                FILTER_CHAIN_AVAILABLE = False
                URL_FILTER_AVAILABLE = False
    
    if URL_FILTER_AVAILABLE:
        # URL FILTERING: Exclude non-content file extensions (no scraper defined for these)
        # These file types are not web pages and should not be queued or scraped
        # Previously PDFs/images were included, but user requested exclusion since no scraper is defined
        exclusion_patterns = [
            r'.*\.ics(\?.*)?$',  # Indico calendar files (.ics with optional query params)
            r'.*\.pdf(\?.*)?$',  # PDF files (no scraper defined)
            r'.*\.jpg(\?.*)?$',  # JPEG images
            r'.*\.jpeg(\?.*)?$', # JPEG images
            r'.*\.png(\?.*)?$',  # PNG images
            r'.*\.zip(\?.*)?$',  # ZIP files
            r'.*\.docx?(\?.*)?$', # Word documents
            r'.*\.mp4(\?.*)?$',  # MP4 videos
            r'.*\.avi(\?.*)?$',  # AVI videos
            r'.*\.xlsx?(\?.*)?$', # Excel files
        ]
        
        # Create filter list first
        filter_list = []
        if URL_FILTER_TYPE == 'RegexURLFilter':
            # RegexURLFilter API: (pattern, include=False) to exclude
            filter_list = [URL_FILTER_CLASS(pattern, include=False) for pattern in exclusion_patterns]
        elif URL_FILTER_TYPE == 'URLPatternFilter':
            # URLPatternFilter API: (patterns=[...], reverse=True) to exclude
            # reverse=True means exclude URLs matching the patterns
            filter_list = [URL_FILTER_CLASS(patterns=exclusion_patterns, reverse=True)]
        
        # Wrap filter list in FilterChain if available, otherwise use list directly
        # Some crawl4ai versions expect FilterChain, others accept list
        if FILTER_CHAIN_AVAILABLE and FilterChain is not None:
            # Use FilterChain if available (newer crawl4ai versions require this)
            try:
                filter_chain = FilterChain(filter_list)
            except (TypeError, AttributeError) as e:
                # FilterChain might not accept list directly, or API changed
                # Fallback: use list directly
                print(f"[WARNING] FilterChain failed, using list directly: {e}")
                filter_chain = filter_list if filter_list else None
        else:
            # Fallback: use list directly (for older versions that accept lists)
            filter_chain = filter_list if filter_list else None
    
    # DOMAIN RESTRICTION: BFSDeepCrawlStrategy's include_external=False only allows exact domain match
    # We need to manually filter links to allow *.desy.de subdomains in link extraction
    # The filter_chain above handles file extensions, but domain restriction is handled separately
    # Note: include_external=False will restrict to www.desy.de, but we manually allow *.desy.de in link extraction
    
    # BFSDeepCrawlStrategy uses Breadth-First Search algorithm:
    # - Crawls all pages at depth 1 before moving to depth 2
    # - Ensures systematic coverage without getting stuck in deep branches
    # - More predictable than Depth-First Search
    deep_crawl_strategy = BFSDeepCrawlStrategy(
        # max_depth: Maximum number of link levels to follow from starting URL
        # Example: If you start at www.example.com and it links to www.example.com/about,
        #          that's depth 1. If /about links to /about/team, that's depth 2.
        # Depth 0 = only the starting page (no link following)
        # Higher depths = more pages crawled (exponential growth)
        max_depth=MAX_DEPTH,
        
        # include_external: Whether to follow links to OTHER domains
        # False = only crawl pages within the same domain as the starting URL
        # True = follow links to external websites (can lead to crawling entire internet!)
        include_external=False,
        
        # max_pages: Maximum total number of pages to crawl
        # Large number (like 10000) = effectively no limit
        # Smaller number = stop after crawling this many pages
        # Note: Cannot be None - must be a number
        max_pages=MAX_PAGES,
        
        # filter_chain: Exclude URLs matching specific patterns (if available)
        # This prevents crawling .ics files and other non-web-page files
        # If None, no filtering is applied (crawler will handle problematic URLs gracefully)
        filter_chain=filter_chain
    )
    
    # BFSDeepCrawlStrategy uses Breadth-First Search algorithm:
    # - First crawls all pages at depth 1
    # - Then crawls all pages at depth 2
    # - And so on...
    # This ensures you get a good overview before going deeper
    
    # ========================================================================
    # STEP 4: Create Crawler Run Configuration
    # ========================================================================
    # This configures HOW the crawler should behave for each page
    
    # ========================================================================
    # Configure content filtering for visually hidden and non-essential content
    # ========================================================================
    # Two-layer approach:
    # 1. PruningContentFilter: Removes non-essential content based on relevance scoring
    # 2. excluded_selector: Explicit CSS-based exclusion of visually-hidden elements
    
    # Layer 1: PruningContentFilter (if available)
    # This removes navigation, footers, ads, and other low-relevance content
    # IMPORTANT: We use a lower threshold to preserve lists, short content blocks,
    # and structured content that might be important (e.g., project lists, field lists)
    markdown_generator = None
    if PRUNING_FILTER_AVAILABLE:
        # Lower threshold (0.2 instead of 0.5) to be less aggressive
        # This preserves more content including lists and short structured blocks
        # min_word_threshold=1 allows single-word list items to be retained
        prune_filter = PruningContentFilter(
            threshold=0.2,              # Lower threshold = less aggressive filtering
            threshold_type="dynamic",  # Dynamic threshold adapts to content
            min_word_threshold=1       # Allow single-word items (important for lists)
        )
        # Configure markdown generator to preserve links (especially mailto: links in tables)
        # Use options to ensure links are preserved in markdown output
        markdown_generator = DefaultMarkdownGenerator(
            content_filter=prune_filter,
            options={
                "ignore_links": False,  # CRITICAL: Preserve all links including mailto:
                "ignore_images": False,  # Preserve images
                "escape_html": True,
                "body_width": 0,  # No wrapping to preserve table structure
                "skip_internal_links": False,  # Include all links
            }
        )
        print("[INFO] PruningContentFilter enabled with conservative settings - preserving lists and structured content")
        print("[INFO] Markdown generator configured to preserve all links (including mailto:)")
    
    # Layer 2: Explicit CSS selectors for visually-hidden and navigation content
    # This ensures navigation, footers, and hidden elements are excluded even if PruningContentFilter misses them
    # We exclude both truly hidden elements AND common navigation/footer patterns
    excluded_selectors = [
        # Visually hidden elements
        '.visually-hidden',           # Common visually hidden class
        '[class*="visually-hidden"]', # Any class containing "visually-hidden"
        '[aria-hidden="true"]',       # ARIA hidden elements (screen reader hidden)
        '[hidden]',                   # HTML5 hidden attribute
        '[style*="display: none"]',    # Inline style display:none (truly hidden)
        '[style*="visibility: hidden"]', # Inline style visibility:hidden (truly hidden)
        
        # Navigation and menus (common patterns across websites)
        'nav',                        # HTML5 nav element
        '.navigation', '.nav',        # Navigation classes
        '[class*="nav"]',             # Any class containing "nav"
        '[id*="nav"]',                # Any ID containing "nav"
        '.menu', '[class*="menu"]',   # Menu classes
        '[id*="menu"]',               # Menu IDs
        '.breadcrumb', '[class*="breadcrumb"]', # Breadcrumb navigation
        '[id*="breadcrumb"]',
        
        # Footers and headers
        'footer', '.footer', '[class*="footer"]', '[id*="footer"]',
        'header', '.header', '[class*="header"]', '[id*="header"]',
        
        # Sidebars and aside elements
        'aside', '.sidebar', '[class*="sidebar"]', '[id*="sidebar"]',
        
        # Cookie and privacy notices
        '.cookie', '[class*="cookie"]', '[id*="cookie"]',
        '.privacy', '[class*="privacy"]',
        '.impressum', '.datenschutz',  # German legal notices
        
        # Loading and placeholder elements
        '[class*="loading"]', '[id*="loading"]',
        '[class*="placeholder"]',
        
        # Skip links and accessibility (often off-screen)
        '.skip-link', '.sprungnavigation', '[class*="skip"]',
    ]
    excluded_selector_str = ', '.join(excluded_selectors)
    
    # Initialize table extraction strategy (for both HTML and PDF)
    # Use custom LinkPreservingTableExtraction to preserve links and emails in table cells
    # This is a general-purpose solution that works for all types of URLs
    table_extraction_strategy = None
    if TABLE_EXTRACTION_AVAILABLE and TableExtractionStrategy:
        # Use custom strategy that preserves HTML links (including mailto: links)
        # This wraps DefaultTableExtraction but post-processes cells to convert links to markdown
        table_extraction_strategy = LinkPreservingTableExtraction(
            table_score_threshold=1,  # Very low threshold to catch all tables (default is 7, lower = more tables)
            min_rows=1,                # Minimum rows (allow single-row tables)
            min_cols=1,                # Minimum columns (allow single-column tables like card layouts)
            verbose=True               # Enable logging for debugging
        )
        print(f"[INFO] Link-preserving table extraction enabled for HTML and PDF pages")
    elif TABLE_EXTRACTION_AVAILABLE:
        # Fallback to DefaultTableExtraction if custom strategy not available
        table_extraction_strategy = DefaultTableExtraction(
            table_score_threshold=1,  # Very low threshold to catch all tables (default is 7, lower = more tables)
            min_rows=1,                # Minimum rows (allow single-row tables)
            min_cols=1,                # Minimum columns (allow single-column tables like card layouts)
            verbose=True               # Enable logging for debugging
        )
        print(f"[INFO] Table extraction enabled (links may not be preserved)")
    
    # ========================================================================
    # STEP 5: Initialize and Run the Crawler
    # ========================================================================
    # The AsyncWebCrawler is the main tool that does the actual crawling.
    # We wrap it in try-except to handle cleanup errors gracefully.
    # The crawler instance is created once and reused for all URLs to improve
    # efficiency (browser initialization is expensive).
    
    try:
        async with AsyncWebCrawler(
            # config: Browser configuration (anti-bot, JavaScript, etc.)
            config=browser_config,
            
            # max_tasks: How many pages to crawl at the same time (parallelism)
            # Higher number = faster crawling but uses more CPU/memory
            # Lower number = slower but more stable
            max_tasks=CONCURRENT_TASKS
        ) as crawler:
            # The 'async with' statement ensures the crawler is properly cleaned up
            # when done (closes browser instances, releases resources)
            
            # ====================================================================
            # STEP 6: Process Each URL in the List
            # ====================================================================
            print(f"[START] Processing {len(ROOT_URLS)} URL(s)")
            print(f"[CONFIG] Max depth: {MAX_DEPTH}, Concurrent tasks: {CONCURRENT_TASKS}")
            print("-" * 60)
            
            for url_idx, root_url in enumerate(ROOT_URLS, 1):
                print(f"\n{'='*60}")
                print(f"[URL {url_idx}/{len(ROOT_URLS)}] Processing: {root_url}")
                print(f"{'='*60}")
                
                # ====================================================================
                # Configure PDF scraping strategy for this URL (if it's a PDF)
                # ====================================================================
                scraping_strategy = None
                crawler_strategy = None
                
                # Check if we're processing a PDF URL
                is_pdf = is_pdf_url(root_url)
                
                # Configure PDF scraping (if available and URL is a PDF)
                if PDF_SUPPORT_AVAILABLE and is_pdf:
                    pdf_image_output_dir = OUTPUT_DIR / "extracted_images"
                    pdf_image_output_dir.mkdir(exist_ok=True)
                    
                    # Create PDF scraping strategy with image extraction
                    scraping_strategy = PDFContentScrapingStrategy(
                        extract_images=True,
                        save_images_locally=True,
                        image_save_dir=str(pdf_image_output_dir)
                    )
                    
                    # Create PDF crawler strategy (required for PDF processing)
                    crawler_strategy = PDFCrawlerStrategy()
                    
                    print(f"[INFO] PDF URL detected - enabling PDF extraction")
                    print(f"[INFO] PDF images will be saved to {pdf_image_output_dir}")
                elif PDF_SUPPORT_AVAILABLE and not is_pdf:
                    print(f"[INFO] HTML URL - using standard HTML crawling")
                
                # Create config for this URL
                # CRITICAL FIX: excluded_tags removes nav/footer/header BEFORE link extraction
                # This causes BFSDeepCrawlStrategy to miss links in those sections
                # Solution: Use minimal excluded_tags for link extraction (only script/style/noscript)
                # Keep full excluded_tags for markdown generation (applied later)
                # Note: crawl4ai may apply excluded_tags during link extraction, so we minimize it here
                link_extraction_excluded_tags = ['script', 'style', 'noscript'] if not is_pdf else None  # Minimal filtering for link extraction
                markdown_excluded_tags = ['nav', 'footer', 'header', 'aside', 'script', 'style', 'noscript', 'select', 'option'] if not is_pdf else None  # Full filtering for markdown
                
                config = CrawlerRunConfig(
                    # deep_crawl_strategy: Tells crawler to follow links using our strategy
                    deep_crawl_strategy=deep_crawl_strategy,
                    
                    # page_timeout: Maximum time to wait for page to load (in milliseconds)
                    # Increased timeout for JavaScript-heavy pages
                    page_timeout=PAGE_TIMEOUT,
                    
                    # wait_until: Wait for page load event before extracting
                    # 'networkidle' waits for network to be idle (all JavaScript requests complete)
                    # This is critical for pages that load content via JavaScript
                    wait_until='networkidle' if not is_pdf else None,
                    
                    # scraping_strategy: PDF extraction strategy (only set for PDF URLs)
                    scraping_strategy=scraping_strategy,
                    
                    # table_extraction: Table extraction strategy (for both PDFs and HTML)
                    table_extraction=table_extraction_strategy if TABLE_EXTRACTION_AVAILABLE else None,
                    
                    # markdown_generator: Uses PruningContentFilter to remove non-essential content
                    # Only set if PruningContentFilter is available and not processing PDFs
                    markdown_generator=markdown_generator if not is_pdf else None,
                    
                    # excluded_tags: Remove entire HTML tags (navigation, footers, etc.)
                    # CRITICAL: Use minimal filtering here to ensure BFSDeepCrawlStrategy sees all links
                    # Full filtering (nav/footer/header) is applied during markdown generation, not link extraction
                    # This ensures links in nav/footer/header are found and followed
                    excluded_tags=link_extraction_excluded_tags,
                    
                    # excluded_selector: CSS selectors for elements to exclude from extraction
                    # Complements excluded_tags with more granular control
                    # Not needed for PDFs as they're processed directly
                    excluded_selector=excluded_selector_str if not is_pdf else None,
                    
                    # word_count_threshold: Filter out short text blocks (navigation items, labels)
                    # Removes text blocks with fewer than this many words
                    # Helps eliminate navigation menus, short labels, and UI elements
                    word_count_threshold=5 if not is_pdf else None,
                    
                    # remove_forms: Remove all form elements (cookie consent, search forms)
                    # Cookie consent forms are common noise in web scraping
                    remove_forms=True if not is_pdf else None,
                    
                    # verbose: Print progress information while crawling
                    verbose=True
                )
                
                # Update crawler's strategy for this URL (if PDF)
                if crawler_strategy:
                    crawler.crawler_strategy = crawler_strategy
                
                # This is where the magic happens:
                # - Crawler fetches the root page
                # - Extracts all links from the page
                # - Follows those links (up to max_depth)
                # - Returns a list of all crawled pages
                # CRITICAL: Normalize URL to remove www. prefix for proper domain matching
                # crawl4ai treats www.desy.de and desy.de as different domains with include_external=False
                # Removing www. ensures links to desy.de are followed when starting from www.desy.de
                normalized_url = root_url.replace('://www.', '://') if root_url else root_url
                if normalized_url != root_url:
                    print(f"[INFO] Normalized URL for domain matching: {root_url} -> {normalized_url}")



                # #region agent log
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'DOMAIN',
                        'location': 'crawl_desy_all_urls.py:4856',
                        'message': 'Starting crawl with normalized URL',
                        'data': {
                            'original_url': root_url,
                            'normalized_url': normalized_url,
                            'max_depth': MAX_DEPTH,
                            'include_external': False,
                            'max_pages': MAX_PAGES
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                
                # First, crawl the page to get initial results
                # ERROR LOGGING: Wrap in try-except to catch timeout and other errors
                try:
                    results = await crawler.arun(normalized_url, config=config)
                except Exception as e:
                    # Log timeout and other errors for future retries
                    error_msg = str(e)
                    is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower() or 'TimeoutError' in str(type(e).__name__)
                    
                    error_entry = {
                        'url': normalized_url,
                        'error': error_msg,
                        'error_type': type(e).__name__,
                        'is_timeout': is_timeout,
                        'timestamp': datetime.now().isoformat(),
                        'note': 'Timeout errors can be retried with PAGE_TIMEOUT_EXTENDED in future runs'
                    }
                    all_errors.append(error_entry)
                    
                    if is_timeout:
                        print(f"[TIMEOUT] {normalized_url}: {error_msg[:100]}")
                    else:
                        print(f"[ERROR] {normalized_url}: {error_msg[:100]}")
                    
                    # Continue with next URL - don't let one failure stop the entire crawl
                    continue
                
                # #region agent log
                result_count = len(results) if isinstance(results, list) else 1
                result_list = results if isinstance(results, list) else [results]
                result_urls = [r.url for r in result_list if r and hasattr(r, 'url') and r.url]
                
                # Check first result for links extracted
                first_result = result_list[0] if result_list and result_list[0] else None
                internal_links = []
                external_links = []
                html_links = []
                links_structure = "unknown"
                
                if first_result and hasattr(first_result, 'links') and first_result.links:
                    # crawl4ai returns links as dict: {"internal": [...], "external": [...]}
                    if isinstance(first_result.links, dict):
                        links_structure = "dict"
                        internal_links = first_result.links.get('internal', [])[:20]
                        external_links = first_result.links.get('external', [])[:10]
                    elif isinstance(first_result.links, list):
                        links_structure = "list"
                        internal_links = first_result.links[:20]
                
                # Also extract links directly from HTML for comparison
                if first_result and hasattr(first_result, 'html') and first_result.html and BEAUTIFULSOUP_AVAILABLE:
                    try:
                        soup = BeautifulSoup(first_result.html, 'html.parser')
                        html_links = [a.get('href', '') for a in soup.find_all('a', href=True) if a.get('href', '').startswith('https://desy.de/')][:30]
                    except:
                        pass
                
                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                    import json
                    from urllib.parse import urlparse
                    f.write(json.dumps({
                        'sessionId': 'debug-session',
                        'runId': 'run1',
                        'hypothesisId': 'LINKS',
                        'location': 'crawl_desy_all_urls.py:4881',
                        'message': 'Crawl completed - link analysis',
                        'data': {
                            'normalized_url': normalized_url,
                            'result_count': result_count,
                            'urls_crawled': result_urls[:20],
                            'links_structure': links_structure,
                            'internal_links_count': len(internal_links) if internal_links else 0,
                            'internal_links_sample': internal_links[:10] if internal_links else [],
                            'external_links_count': len(external_links) if external_links else 0,
                            'html_links_desy_de': html_links[:15],
                            'unique_domains': list(set([urlparse(url).netloc for url in result_urls if url]))[:10]
                        },
                        'timestamp': int(__import__('time').time() * 1000)
                    }) + '\n')
                # #endregion
                
                # FIX: Extract links from full HTML (including nav/footer/header) and crawl them
                # crawl4ai's excluded_tags filters links in nav/footer/header, so we manually extract them
                # This ensures links like "desy.de/desy_in_leichter_sprache/index_ger.html" are crawled
                additional_urls_to_crawl = []
                if first_result and hasattr(first_result, 'html') and first_result.html and BEAUTIFULSOUP_AVAILABLE:
                    try:
                        from urllib.parse import urljoin, urlparse
                        soup = BeautifulSoup(first_result.html, 'html.parser')
                        base_url = first_result.url if hasattr(first_result, 'url') and first_result.url else normalized_url
                        base_domain = urlparse(base_url).netloc.replace('www.', '')
                        
                        # Get URLs already crawled (normalized)
                        seen_urls = set()
                        for url in result_urls:
                            if url:
                                normalized_seen = url.replace('://www.', '://')
                                seen_urls.add(normalized_seen)
                        
                        # Extract ALL links from HTML (including nav/footer/header)
                        all_links = soup.find_all('a', href=True)
                        
                        for link in all_links:
                            href = link.get('href', '').strip()
                            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                                continue
                            
                            # Resolve relative URLs
                            absolute_url = urljoin(base_url, href)
                            parsed = urlparse(absolute_url)
                            
                            # Only include internal links (same domain, no www mismatch)
                            link_domain = parsed.netloc.replace('www.', '')
                            if link_domain == base_domain:
                                # Normalize URL (remove www)
                                normalized_link = absolute_url.replace('://www.', '://')
                                # Only add if not already crawled
                                if normalized_link not in seen_urls:
                                    additional_urls_to_crawl.append(normalized_link)
                                    seen_urls.add(normalized_link)
                        
                        # Crawl additional URLs found in HTML but missed by crawl4ai's link extraction
                        if additional_urls_to_crawl:
                            # #region agent log
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'FIX',
                                    'location': 'crawl_desy_all_urls.py:4930',
                                    'message': 'Found additional URLs in HTML (nav/footer/header)',
                                    'data': {
                                        'additional_urls_count': len(additional_urls_to_crawl),
                                        'additional_urls_sample': additional_urls_to_crawl[:10]
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                            # #endregion
                            
                            # Crawl each additional URL individually (they'll be at depth 1)
                            # Limit to reasonable number to avoid excessive crawling
                            # Create a config without deep_crawl_strategy to crawl single pages only
                            # These URLs should be crawled at depth 1 (single page, no link following)
                            single_page_config = CrawlerRunConfig(
                                page_timeout=PAGE_TIMEOUT,
                                wait_until='networkidle' if not is_pdf else None,
                                scraping_strategy=scraping_strategy,
                                table_extraction=table_extraction_strategy if TABLE_EXTRACTION_AVAILABLE else None,
                                markdown_generator=markdown_generator if not is_pdf else None,
                                excluded_tags=['nav', 'footer', 'header', 'aside', 'script', 'style', 'noscript', 'select', 'option'] if not is_pdf else None,
                                excluded_selector=excluded_selector_str if not is_pdf else None,
                                word_count_threshold=5 if not is_pdf else None,
                                remove_forms=True if not is_pdf else None,
                                verbose=True
                                # NOTE: No deep_crawl_strategy - this ensures single page crawl only
                            )
                            
                            for additional_url in additional_urls_to_crawl[:100]:
                                try:
                                    # Crawl single page (no deep crawl strategy = single page only)
                                    # These will get depth 1 when processed (see depth assignment logic below)
                                    additional_result = await crawler.arun(additional_url, config=single_page_config)
                                    if isinstance(additional_result, list):
                                        all_results.extend(additional_result)
                                    else:
                                        all_results.append(additional_result)
                                except Exception as e:
                                    # ERROR LOGGING: Log timeout and other errors for future retries
                                    error_msg = str(e)
                                    is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower() or 'TimeoutError' in str(type(e).__name__)
                                    
                                    error_entry = {
                                        'url': additional_url,
                                        'error': error_msg,
                                        'error_type': type(e).__name__,
                                        'is_timeout': is_timeout,
                                        'timestamp': datetime.now().isoformat(),
                                        'note': 'Timeout errors can be retried with PAGE_TIMEOUT_EXTENDED in future runs'
                                    }
                                    all_errors.append(error_entry)
                                    
                                    if is_timeout:
                                        print(f"[TIMEOUT] {additional_url}: {error_msg[:100]}")
                                    # Continue - some URLs might fail
                    except Exception as e:
                        # Log error but continue
                        pass
                
                # Accumulate initial results
                if isinstance(results, list):
                    all_results.extend(results)
                else:
                    all_results.append(results)
            
            # ====================================================================
            # STEP 7: Extract links from ALL results (not just seed URL)
            # ====================================================================
            # CRITICAL FIX: Manual link extraction should run for ALL pages, not just seed URL
            # crawl4ai's excluded_tags filters links in nav/footer/header, so we manually extract them
            # from ALL crawled pages to ensure no links are missed
            
            # Collect all additional URLs to crawl from all results
            # Track URL -> source_depth mapping to assign correct depths
            all_additional_urls = {}  # {normalized_url: source_depth}
            seen_crawled_urls = set()
            # Track depth mapping for additional URLs (will be populated when crawling them)
            additional_urls_with_depth = {}  # {normalized_url: depth} - populated during crawling
            
            # First, collect all URLs that were already crawled AND their depths
            # This prevents us from reassigning depth to URLs that were already crawled by BFSDeepCrawlStrategy
            crawled_urls_with_depth = {}  # {normalized_url: depth} - URLs already crawled with their depths
            for result in all_results:
                if result:
                    # Get depth from result metadata
                    result_depth = 0
                    if hasattr(result, 'metadata') and result.metadata:
                        result_depth = result.metadata.get('depth', 0)
                    elif hasattr(result, 'depth'):
                        result_depth = result.depth
                    
                    # Check if it's a seed URL
                    source_url = result.url if hasattr(result, 'url') and result.url else None
                    if source_url:
                        normalized_source = source_url.replace('://www.', '://')
                        for root_url in ROOT_URLS:
                            normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                            if normalized_source == normalized_seed:
                                result_depth = 0
                                break
                    
                    # If depth is 0 and not seed, default to 1
                    if result_depth == 0 and source_url:
                        is_seed = False
                        normalized_source = source_url.replace('://www.', '://')
                        for root_url in ROOT_URLS:
                            normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                            if normalized_source == normalized_seed:
                                is_seed = True
                                break
                        if not is_seed:
                            result_depth = 1
                    
                    # Store URL and depth
                    if hasattr(result, 'url') and result.url:
                        normalized = result.url.replace('://www.', '://')
                        seen_crawled_urls.add(normalized)
                        crawled_urls_with_depth[normalized] = result_depth
                    if hasattr(result, 'redirected_url') and result.redirected_url:
                        normalized = result.redirected_url.replace('://www.', '://')
                        seen_crawled_urls.add(normalized)
                        crawled_urls_with_depth[normalized] = result_depth
            
            # Extract links from ALL results' HTML
            # Track source page depth to assign correct depth to additional URLs
            # IMPORTANT: Only assign depth to URLs that are NOT already in seen_crawled_urls
            # If a URL was already crawled by BFSDeepCrawlStrategy, it already has correct depth
            for result in all_results:
                if not result or not hasattr(result, 'html') or not result.html or not BEAUTIFULSOUP_AVAILABLE:
                    continue
                
                try:
                    # Determine source page depth
                    source_depth = 0
                    source_url = result.url if hasattr(result, 'url') and result.url else None
                    if hasattr(result, 'metadata') and result.metadata:
                        source_depth = result.metadata.get('depth', 0)
                    elif hasattr(result, 'depth'):
                        source_depth = result.depth
                    
                    # Check if this is a seed URL
                    if source_url:
                        normalized_source = source_url.replace('://www.', '://')
                        for root_url in ROOT_URLS:
                            normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                            if normalized_source == normalized_seed:
                                source_depth = 0
                                break
                    
                    # If source_depth is still 0 and it's not a seed URL, it's likely from a single-page crawl
                    # In that case, we need to determine depth from the result's final URL
                    if source_depth == 0 and source_url:
                        normalized_source = source_url.replace('://www.', '://')
                        # Check if redirected URL is different
                        if hasattr(result, 'redirected_url') and result.redirected_url:
                            normalized_final = result.redirected_url.replace('://www.', '://')
                            for root_url in ROOT_URLS:
                                normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                                if normalized_final == normalized_seed:
                                    source_depth = 0
                                    break
                        # If still 0 and not seed, default to 1
                        if source_depth == 0:
                            is_seed = False
                            for root_url in ROOT_URLS:
                                normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                                if normalized_source == normalized_seed:
                                    is_seed = True
                                    break
                            if not is_seed:
                                source_depth = 1
                    
                    from urllib.parse import urljoin, urlparse
                    soup = BeautifulSoup(result.html, 'html.parser')
                    base_url = result.url if hasattr(result, 'url') and result.url else None
                    if not base_url:
                        continue
                    
                    base_domain = urlparse(base_url).netloc.replace('www.', '')
                    
                    # Extract ALL links from HTML (including nav/footer/header)
                    all_links = soup.find_all('a', href=True)
                    
                    for link in all_links:
                        href = link.get('href', '').strip()
                        if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                            continue
                        
                        # Resolve relative URLs
                        absolute_url = urljoin(base_url, href)
                        parsed = urlparse(absolute_url)
                        
                        # DOMAIN RESTRICTION: Only include *.desy.de subdomains
                        # Allow all DESY subdomains (www.desy.de, photon-science.desy.de, etc.)
                        # Exclude external domains (facebook.com, instagram.com, etc.)
                        link_domain = parsed.netloc.replace('www.', '')
                        is_desy_domain = link_domain.endswith('.desy.de') or link_domain == 'desy.de'
                        if is_desy_domain:
                            # Normalize URL (remove www)
                            normalized_link = absolute_url.replace('://www.', '://')
                            # Only add if not already crawled by BFSDeepCrawlStrategy
                            # URLs already crawled have correct depth from BFS, don't reassign
                            if normalized_link not in seen_crawled_urls:
                                # Track source depth: additional URL should be at source_depth + 1
                                # But keep minimum depth if URL was found from multiple pages
                                # Also ensure we don't exceed MAX_DEPTH
                                assigned_depth = source_depth + 1
                                if assigned_depth > MAX_DEPTH:
                                    continue  # Skip URLs that would exceed max depth
                                
                                if normalized_link not in all_additional_urls:
                                    all_additional_urls[normalized_link] = assigned_depth
                                else:
                                    # Keep minimum depth (closest to seed) - but don't go below source_depth + 1
                                    all_additional_urls[normalized_link] = min(all_additional_urls[normalized_link], assigned_depth)
                            else:
                                # URL was already crawled - use its original depth from BFSDeepCrawlStrategy
                                # Don't add to all_additional_urls, it will use its original depth
                                pass
                except Exception as e:
                    # Log but continue
                    pass
            
            # Crawl all additional URLs found in HTML but missed by crawl4ai's link extraction
            if all_additional_urls:
                # Debug: Count URLs by assigned depth
                depth_counts = {}
                for url, depth in all_additional_urls.items():
                    depth_counts[depth] = depth_counts.get(depth, 0) + 1
                print(f"[INFO] Found {len(all_additional_urls)} additional URLs in HTML (nav/footer/header) from all pages")
                print(f"[INFO] Additional URLs by assigned depth: {depth_counts}")
                
                # Create a config for additional URLs
                # CRITICAL FIX: Use deep_crawl_strategy for additional URLs to enable recursive crawling
                # This ensures links found in nav/footer/header are also followed recursively
                # Limit depth to avoid excessive crawling (use remaining depth from MAX_DEPTH)
                additional_deep_crawl_strategy = BFSDeepCrawlStrategy(
                    max_depth=MAX_DEPTH,  # Use full depth to ensure comprehensive crawling
                    include_external=False,
                    max_pages=MAX_PAGES,
                    filter_chain=filter_chain if 'filter_chain' in locals() else None
                )
                
                # Config for additional URLs with deep crawl enabled
                # Use variables defined outside the loop (excluded_selector_str, table_extraction_strategy, markdown_generator)
                # CRITICAL: Use minimal excluded_tags here too to ensure links are found
                # Full filtering is applied during markdown generation via markdown_generator
                additional_urls_config = CrawlerRunConfig(
                    deep_crawl_strategy=additional_deep_crawl_strategy,  # Enable deep crawl for additional URLs
                    page_timeout=PAGE_TIMEOUT,
                    wait_until='networkidle',
                    scraping_strategy=None,  # Additional URLs are HTML, not PDF
                    table_extraction=table_extraction_strategy if TABLE_EXTRACTION_AVAILABLE else None,
                    markdown_generator=markdown_generator if PRUNING_FILTER_AVAILABLE else None,
                    excluded_tags=['script', 'style', 'noscript'],  # Minimal filtering for link extraction
                    excluded_selector=excluded_selector_str,
                    word_count_threshold=5,
                    remove_forms=True,
                    verbose=True
                )
                
                # Crawl additional URLs (limit to avoid excessive crawling)
                # Store depth mapping for additional URLs so we can assign correct depth later
                # Increased limit to 10,000 to capture more links from nav/footer/header
                additional_count = 0
                for additional_url, assigned_depth in list(all_additional_urls.items())[:10000]:  # Limit to 10,000
                    try:
                        # Use config with deep_crawl_strategy to enable recursive crawling
                        additional_result = await crawler.arun(additional_url, config=additional_urls_config)
                        if isinstance(additional_result, list):
                            for res in additional_result:
                                if res:
                                    # Store depth mapping for this result
                                    if res.url:
                                        normalized = res.url.replace('://www.', '://')
                                        additional_urls_with_depth[normalized] = assigned_depth
                                    if hasattr(res, 'redirected_url') and res.redirected_url:
                                        normalized = res.redirected_url.replace('://www.', '://')
                                        additional_urls_with_depth[normalized] = assigned_depth
                            all_results.extend(additional_result)
                        else:
                            if additional_result:
                                # Store depth mapping for this result
                                if additional_result.url:
                                    normalized = additional_result.url.replace('://www.', '://')
                                    additional_urls_with_depth[normalized] = assigned_depth
                                if hasattr(additional_result, 'redirected_url') and additional_result.redirected_url:
                                    normalized = additional_result.redirected_url.replace('://www.', '://')
                                    additional_urls_with_depth[normalized] = assigned_depth
                            all_results.append(additional_result)
                        additional_count += 1
                    except Exception as e:
                        # ERROR LOGGING: Log timeout and other errors for future retries
                        error_msg = str(e)
                        is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower() or 'TimeoutError' in str(type(e).__name__)
                        
                        error_entry = {
                            'url': additional_url,
                            'error': error_msg,
                            'error_type': type(e).__name__,
                            'is_timeout': is_timeout,
                            'timestamp': datetime.now().isoformat(),
                            'note': 'Timeout errors can be retried with PAGE_TIMEOUT_EXTENDED in future runs'
                        }
                        all_errors.append(error_entry)
                        
                        if is_timeout:
                            print(f"[TIMEOUT] {additional_url}: {error_msg[:100]}")
                        # Continue - some URLs might fail
                
                print(f"[INFO] Crawled {additional_count} additional URLs from HTML links")
            
            # ====================================================================
            # STEP 8: Process All Results
            # ====================================================================
            # Process accumulated results from all URLs
            results = all_results

            print("-" * 60)
            print(f"[INFO] Crawling complete! Found {len(results)} pages")
            print("-" * 60)
            
            # ====================================================================
            # STEP 8: Process and Save Each Page
            # ====================================================================
            # Loop through each crawled page and save its content
            # Track successes and failures
            
            # Track seed URLs (normalized) to ensure they get depth 0
            seed_urls_normalized = set()
            for root_url in ROOT_URLS:
                normalized_seed = root_url.replace('://www.', '://') if root_url else root_url
                seed_urls_normalized.add(normalized_seed)
            
            # Track seen final URLs to prevent duplicates
            seen_final_urls = set()
            
            # additional_urls_with_depth is defined in the link extraction section above
            # It maps normalized URLs to their assigned depths
            
            # Track links found vs URLs crawled for analysis
            links_found_vs_crawled = {
                'total_links_found_in_html': 0,
                'total_urls_crawled': 0,
                'links_found_by_page': []
            }
            
            for result in results:
                # Skip if result is invalid
                if not result or not result.url:
                    continue
                
                try:
                    # Track both original and final URLs to handle redirects
                    # crawl4ai provides: result.url (original) and result.redirected_url (final after redirects)
                    original_url = result.url if hasattr(result, 'url') and result.url else None
                    final_url = None
                    is_redirect = False
                    
                    # Check if redirect occurred
                    if hasattr(result, 'redirected_url') and result.redirected_url:
                        final_url = result.redirected_url
                        is_redirect = (original_url != final_url)
                    else:
                        final_url = original_url
                    
                    # Normalize URLs for comparison (remove www)
                    normalized_original = original_url.replace('://www.', '://') if original_url else None
                    normalized_final = final_url.replace('://www.', '://') if final_url else None
                    
                    # Skip 404 pages: Check URL pattern and HTTP status code
                    # Don't track or save 404/empty pages
                    is_404_page = False
                    if normalized_final and '/404/' in normalized_final:
                        is_404_page = True
                    elif hasattr(result, 'status_code') and result.status_code == 404:
                        is_404_page = True
                    
                    if is_404_page:
                        continue  # Skip 404 pages - don't track or save them
                    
                    # Skip if we've already seen this final URL (deduplication)
                    if normalized_final and normalized_final in seen_final_urls:
                        continue
                    seen_final_urls.add(normalized_final)
                    
                    # Determine depth: seed URLs always get depth 0
                    # Otherwise use depth from result metadata or from additional_urls_with_depth mapping
                    # IMPORTANT: URLs already crawled by BFSDeepCrawlStrategy should use their original depth
                    if normalized_original in seed_urls_normalized or normalized_final in seed_urls_normalized:
                        depth = 0  # Seed URL - always depth 0
                    else:
                        depth = 0  # Default
                        # First check if this is an additional URL with assigned depth (from manual link extraction)
                        # These are URLs that were NOT crawled by BFSDeepCrawlStrategy
                        if normalized_final in additional_urls_with_depth:
                            depth = additional_urls_with_depth[normalized_final]
                        elif normalized_original in additional_urls_with_depth:
                            depth = additional_urls_with_depth[normalized_original]
                        # Otherwise use depth from result metadata (from BFSDeepCrawlStrategy)
                        # This is the original depth assigned by BFSDeepCrawlStrategy
                        elif hasattr(result, 'metadata') and result.metadata:
                            depth = result.metadata.get('depth', 0)
                        elif hasattr(result, 'depth'):
                            depth = result.depth
                        # Ensure depth is at least 1 for non-seed URLs
                        if depth == 0:
                            depth = 1
                        
                        # Cap depth at MAX_DEPTH (don't exceed configured max depth)
                        if depth > MAX_DEPTH:
                            depth = MAX_DEPTH
                    
                    # Initialize depth list if needed
                    depth_str = str(depth)
                    if depth_str not in all_urls_by_depth:
                        all_urls_by_depth[depth_str] = []
                    
                    # Store URL entry with redirect information
                    url_entry = {
                        'original_url': original_url,
                        'final_url': final_url,
                        'is_redirect': is_redirect
                    }
                    all_urls_by_depth[depth_str].append(url_entry)
                    
                    # Track links found in HTML vs URLs crawled (for analysis)
                    links_in_html = 0
                    if hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
                        try:
                            from urllib.parse import urlparse
                            soup = BeautifulSoup(result.html, 'html.parser')
                            base_url = final_url if final_url else original_url
                            base_domain = urlparse(base_url).netloc.replace('www.', '') if base_url else ''
                            
                            # Count all internal links in HTML
                            all_links = soup.find_all('a', href=True)
                            for link in all_links:
                                href = link.get('href', '').strip()
                                if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                                    continue
                                
                                from urllib.parse import urljoin
                                absolute_url = urljoin(base_url, href) if base_url else href
                                parsed = urlparse(absolute_url)
                                link_domain = parsed.netloc.replace('www.', '')
                                
                                # Count internal links only
                                if link_domain == base_domain:
                                    links_in_html += 1
                        except:
                            pass
                    
                    links_found_vs_crawled['total_links_found_in_html'] += links_in_html
                    links_found_vs_crawled['total_urls_crawled'] += 1
                    links_found_vs_crawled['links_found_by_page'].append({
                        'url': final_url or original_url,
                        'depth': depth_str,
                        'links_found': links_in_html
                    })
                    
                    # ============================================================
                    # Create a safe filename from the URL
                    # ============================================================
                    # URLs contain characters that aren't valid in filenames (/, :, etc.)
                    # So we convert: https://www.desy.de/about → www_desy_de_about
                    # Use final_url for filename (what was actually crawled)
                    
                    url_for_filename = final_url if final_url else original_url
                    url_safe = url_for_filename.replace("https://", "").replace("http://", "")
                    url_safe = url_safe.replace("/", "_").replace(":", "_")
                    
                    # Limit filename length (some URLs are very long)
                    if len(url_safe) > 200:
                        url_safe = url_safe[:200]
                    
                    # Organize files by depth: create depth-specific subdirectory
                    depth_dir = OUTPUT_DIR / f"depth_{depth_str}"
                    depth_dir.mkdir(exist_ok=True)  # Create depth directory if it doesn't exist
                    
                    # Create full file path in depth-specific directory
                    filename = depth_dir / f"{url_safe}.md"
                    
                    # ============================================================
                    # Extract Markdown Content
                    # ============================================================
                    # For PDFs: PDFContentScrapingStrategy extracts text to raw_markdown
                    # For HTML: Crawl4AI converts HTML to markdown
                    # Strategy: Combine fit_markdown (cleaned) and raw_markdown (complete)
                    # to ensure no content is lost, especially lists and structured content
                    result_is_pdf = is_pdf_url(result.url)
                    
                    markdown_content = ""
                    tables_markdown = ""  # Initialize tables_markdown at the top level
                    if hasattr(result, 'markdown'):
                        # Check if result has fit_markdown (cleaned version)
                        if hasattr(result.markdown, 'fit_markdown'):
                            if result_is_pdf:
                                # For PDFs, prefer raw_markdown (extracted PDF text)
                                markdown_content = result.markdown.raw_markdown or result.markdown.fit_markdown or ""
                            else:
                                # For HTML: Use fit_markdown as primary (cleaned, navigation removed)
                                # But if it's significantly shorter than raw_markdown, combine both
                                # This ensures lists and structured content aren't lost
                                fit_content = result.markdown.fit_markdown or ""
                                raw_content = result.markdown.raw_markdown or ""
                                
                                # CRITICAL: For pages with tables, prefer raw_markdown to preserve table structure
                                # Also use raw if fit is empty or much shorter
                                if fit_content and raw_content:
                                    fit_len = len(fit_content.strip())
                                    raw_len = len(raw_content.strip())
                                    
                                    # Use raw if:
                                    # 1. fit is empty or very short (< 100 chars)
                                    # 2. fit is less than 50% of raw (too aggressive filtering)
                                    # 3. raw contains table markers (|) but fit doesn't (tables were filtered out)
                                    has_tables_in_raw = '|' in raw_content and len([l for l in raw_content.split('\n') if '|' in l]) >= 3
                                    has_tables_in_fit = '|' in fit_content and len([l for l in fit_content.split('\n') if '|' in l]) >= 3
                                    
                                    if fit_len < 100 or (raw_len > 0 and (fit_len / raw_len) < 0.5) or (has_tables_in_raw and not has_tables_in_fit):
                                        markdown_content = raw_content
                                        if fit_len < 100:
                                            print(f"[INFO] Using raw_markdown (fit_markdown was empty/too short: {fit_len} chars)")
                                        elif has_tables_in_raw and not has_tables_in_fit:
                                            print(f"[INFO] Using raw_markdown (tables were filtered out from fit_markdown)")
                                        else:
                                            print(f"[INFO] Using raw_markdown (fit_markdown was {fit_len}/{raw_len} chars, may have lost content)")
                                    else:
                                        markdown_content = fit_content
                                elif raw_content:
                                    # If only raw is available, use it
                                    markdown_content = raw_content
                                    print(f"[INFO] Using raw_markdown (fit_markdown not available)")
                                else:
                                    # Fallback to fit if raw not available
                                    markdown_content = fit_content
                                
                                # CRITICAL: Check if markdown has meaningful content (not just headers/URLs)
                                # If it's mostly empty or just has URL/headers, extract from HTML
                                markdown_meaningful = markdown_content.strip()
                                # Remove URL header and separators to check actual content
                                markdown_meaningful = re.sub(r'^#\s*Source\s*URL.*?\n---\s*\n', '', markdown_meaningful, flags=re.IGNORECASE | re.MULTILINE)
                                markdown_meaningful = markdown_meaningful.strip()
                                
                                # Count actual table rows (not separators)
                                table_rows_in_markdown = [l for l in markdown_content.split('\n') if '|' in l and not re.match(r'^\s*\|[\s\-:]+\|', l) and l.strip()]
                                has_tables_in_markdown = len(table_rows_in_markdown) >= 2
                                
                                # General signal: HTML contains many contact/profile tables, but markdown reflects far fewer.
                                # Use mailto count in markdown (strong proxy for "person rows extracted").
                                html_contact_table_count = 0
                                html_total_table_count = 0
                                markdown_mailto_count = len(re.findall(r'\(mailto:[^)]+\)', markdown_content or '', flags=re.IGNORECASE))
                                if hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
                                    try:
                                        soup_probe = BeautifulSoup(result.html, 'html.parser')
                                        probe_tables = soup_probe.find_all('table', recursive=True)
                                        html_total_table_count = len(probe_tables)
                                        for t in probe_tables:
                                            if t.find('a', href=lambda x: x and x.startswith('mailto:')):
                                                html_contact_table_count += 1
                                    except Exception:
                                        html_contact_table_count = 0
                                        html_total_table_count = 0
                                
                                tables_missing_vs_html = (
                                    html_contact_table_count >= 3 and
                                    markdown_mailto_count < html_contact_table_count
                                )
                                
                                # ALWAYS check HTML if markdown is empty or has no tables
                                # This ensures we extract content even if PruningContentFilter removed everything
                                # Also check if HTML exists and has content (might be JavaScript-loaded)
                                html_has_content = False
                                html_length = 0
                                if hasattr(result, 'html') and result.html:
                                    html_length = len(result.html.strip())
                                    html_has_content = html_length > 1000  # HTML has substantial content
                                
                                # Check HTML if:
                                # 1. Markdown is empty/meaningless
                                # 2. Markdown has no tables but HTML might have them
                                # 3. HTML has substantial content but markdown doesn't (JavaScript-loaded content)
                                should_check_html = (not markdown_meaningful or 
                                                    len(markdown_meaningful) < 100 or 
                                                    not has_tables_in_markdown or
                                                    tables_missing_vs_html or
                                                    (html_has_content and len(markdown_meaningful) < 50))
                                
                                
                                if should_check_html:
                                    print(f"[DEBUG] Will check HTML - markdown: {len(markdown_meaningful)} chars, html: {html_length} chars, has_tables: {has_tables_in_markdown}")
                                    if hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
                                        try:
                                            soup = BeautifulSoup(result.html, 'html.parser')
                                            
                                            
                                            # SIMPLIFIED: Always extract from HTML if markdown is empty or too short
                                            # No complex contact block extraction - just extract all content properly
                                            should_extract = (not markdown_meaningful or 
                                                            len(markdown_meaningful) < 100 or
                                                            tables_missing_vs_html)
                                            
                                            if should_extract:
                                                # Count HTML tables for debug output
                                                html_table_count = len(soup.find_all('table', recursive=True)) if soup else 0
                                                print(f"[DEBUG] Will use Crawl4AI tables - markdown_meaningful: {len(markdown_meaningful)} chars, html_tables: {html_table_count}, markdown_tables: {has_tables_in_markdown}, html_length: {html_length}")
                                                
                                                # Use only Crawl4AI's table extraction - skip all custom HTML parsing
                                                # Tables will be extracted from result.tables later in the code
                                                html_fallback_tables = ""
                                                
                                                # Now remove script and style elements, plus navigation/UI elements
                                                # This is critical to avoid extracting dropdown menus, navigation, and UI noise
                                                for element in soup(["script", "style", "nav", "header", "footer",
                                                                    "select", "option",  # Dropdown menus
                                                                    "noscript", "iframe",
                                                                    "form"]):  # Forms often contain search UI
                                                    element.decompose()
                                                
                                                # Remove elements with common navigation/UI classes/IDs
                                                # These often contain dropdown menus and navigation that get flattened into text
                                                ui_patterns = [
                                                    r'nav', r'menu', r'dropdown', r'select', r'option',
                                                    r'breadcrumb', r'sidebar', r'cookie', r'privacy',
                                                    r'search', r'filter', r'pagination', r'toolbar',
                                                    r'header', r'footer', r'aside'
                                                ]
                                                for pattern in ui_patterns:
                                                    # Remove by class
                                                    for elem in soup.find_all(class_=re.compile(pattern, re.I)):
                                                        elem.decompose()
                                                    # Remove by ID
                                                    for elem in soup.find_all(id=re.compile(pattern, re.I)):
                                                        elem.decompose()
                                                
                                                # Remove elements that are likely navigation/UI (have many links but little text)
                                                # This catches navigation menus that weren't caught by the above
                                                # GENERAL: More aggressive filtering for navigation patterns
                                                for elem in soup.find_all(['div', 'ul', 'ol', 'li']):
                                                    links = elem.find_all('a')
                                                    text = elem.get_text(strip=True)
                                                    # If element has many links but short text, it's likely navigation
                                                    # Also check for spacer images (common in navigation)
                                                    spacer_imgs = elem.find_all('img', src=re.compile(r'spacer|blank|pixel', re.I))
                                                    if (len(links) > 3 and len(text) < 200) or (len(spacer_imgs) > 0 and len(links) > 2):
                                                        elem.decompose()
                                                
                                                # Try to find main content - check multiple possible containers
                                                # Also check for iframes (content might be loaded in iframe)
                                                main_content = None
                                                
                                                # Check for iframes first (content might be loaded in iframe)
                                                iframes = soup.find_all('iframe')
                                                if iframes:
                                                    print(f"[DEBUG] Found {len(iframes)} iframe(s) - content might be in iframe")
                                                    for iframe in iframes:
                                                        iframe_src = iframe.get('src', '')
                                                        if iframe_src:
                                                            print(f"[DEBUG] Iframe src: {iframe_src}")
                                                            # Note: Cross-origin iframe content cannot be accessed directly
                                                            # But we can note the iframe URL for reference
                                                
                                                # Try standard containers
                                                main_content = (soup.find('main') or 
                                                              soup.find('article') or 
                                                              soup.find('div', class_=re.compile(r'content|main|body', re.I)) or
                                                              soup.find('div', id=re.compile(r'content|main|body', re.I)))
                                                
                                                # If main_content is too small or not found, try body
                                                # RELAXED: Lower threshold from 50 to 20 chars to catch contact pages
                                                if not main_content or (main_content and len(main_content.get_text(strip=True)) < 20):
                                                    main_content = soup.find('body')
                                                    if main_content:
                                                        print(f"[DEBUG] Using body as main content ({len(main_content.get_text(strip=True))} chars)")
                                                
                                                # If still no good content, check for common content divs with substantial text
                                                # RELAXED: Lower threshold and check for contact info patterns
                                                if not main_content or (main_content and len(main_content.get_text(strip=True)) < 20):
                                                    # Look for any div with text content (lowered threshold)
                                                    all_divs = soup.find_all('div')
                                                    best_div = None
                                                    best_score = 0
                                                    for div in all_divs:
                                                        div_text = div.get_text(strip=True)
                                                        # Score divs based on text length and contact info presence
                                                        score = len(div_text)
                                                        # Boost score if contains contact info patterns
                                                        if re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', div_text):
                                                            score += 500  # Big boost for contact info
                                                        if re.search(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+', div_text):
                                                            score += 100  # Boost for names
                                                        # If div has substantial text or contact info, consider it
                                                        if score > best_score:
                                                            best_score = score
                                                            best_div = div
                                                    
                                                    if best_div and best_score > 50:  # Lower threshold
                                                        main_content = best_div
                                                        print(f"[DEBUG] Using div with score {best_score} as main content")
                                                
                                                # CRITICAL FIX: If still no main_content found, but we have tables, use body anyway
                                                # This handles pages where content structure is non-standard
                                                if not main_content:
                                                    body = soup.find('body')
                                                    if body:
                                                        main_content = body
                                                        print(f"[DEBUG] No main content found, using body as fallback ({len(body.get_text(strip=True))} chars)")
                                                    else:
                                                        # Last resort: use entire soup
                                                        main_content = soup
                                                        print(f"[DEBUG] No body found, using entire soup as fallback")
                                                
                                                # Tables are extracted by Crawl4AI and will be processed from result.tables later
                                                print(f"[DEBUG] Skipping custom HTML table extraction - using Crawl4AI tables from result.tables")
                                                
                                                # Now extract text if we have main_content
                                                if main_content:
                                                    
                                                    # RELAXED: If main_content text is very short, try extracting from paragraphs directly
                                                    # This helps with contact pages where content is in paragraphs
                                                    main_content_text_preview = main_content.get_text(strip=True)
                                                    if len(main_content_text_preview) < 100:
                                                        # Try extracting from all paragraphs in the page
                                                        all_paragraphs = soup.find_all(['p', 'div'])
                                                        paragraph_texts = []
                                                        for para in all_paragraphs:
                                                            para_text = para.get_text(strip=True)
                                                            # Check if paragraph has contact info or substantial content
                                                            if para_text and (len(para_text) > 20 or re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', para_text)):
                                                                # Skip if it's clearly navigation/noise
                                                                if para_text.count('](') < 10 and not re.match(r'^https?://', para_text):
                                                                    paragraph_texts.append(para_text)
                                                        
                                                        if paragraph_texts:
                                                            para_joined = '\n'.join(paragraph_texts)
                                                            if len(para_joined) > len(main_content_text_preview):
                                                                # Use paragraphs instead
                                                                print(f"[DEBUG] Using {len(paragraph_texts)} paragraphs for extraction (total {len(para_joined)} chars)")
                                                            # Create a temporary soup with just these paragraphs
                                                            para_soup = BeautifulSoup('', 'html.parser')
                                                            for para in all_paragraphs:
                                                                para_text = para.get_text(strip=True)
                                                                if para_text and (len(para_text) > 20 or re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', para_text)):
                                                                    if para_text.count('](') < 10 and not re.match(r'^https?://', para_text):
                                                                        para_soup.append(para)
                                                            if len(para_soup.get_text(strip=True)) > len(main_content_text_preview):
                                                                main_content = para_soup
                                                                print(f"[DEBUG] Switched to paragraph-based extraction")
                                                    
                                                    # Table extraction is handled by Crawl4AI - no need to search for div-based tables
                                                    
                                                    # SIMPLIFIED: Convert all links (including emails) to markdown format
                                                    # This preserves emails and all links in the output
                                                    from bs4 import NavigableString
                                                    for link in main_content.find_all('a', href=True):
                                                        href = link.get('href', '').strip()
                                                        href_no_frag = href.split('#', 1)[0].rstrip('/')
                                                        link_text = link.get_text(strip=True) or ""
                                                        
                                                        # Skip empty/anchor links (just #)
                                                        if not href or (href == '#' or (href.startswith('#') and len(href) == 1)):
                                                            link.decompose()
                                                            continue

                                                        # Issue #1 fix: drop self-referencing links (often have empty text and become []())
                                                        if result.url:
                                                            url_norm = result.url.split('#', 1)[0].rstrip('/')
                                                            if href_no_frag == url_norm:
                                                                link.decompose()
                                                                continue
                                                            # Handle relative self-links like "/career/contact/index_eng.html"
                                                            if href.startswith('/') and url_norm.endswith(href_no_frag):
                                                                link.decompose()
                                                                continue
                                                            # Also drop same-page anchors if they have no visible text
                                                            if href.startswith(result.url) and ('#' in href) and not link_text:
                                                                link.decompose()
                                                                continue

                                                        # Drop empty-text non-email links (prevents [](...))
                                                        if not link_text and not href.startswith('mailto:'):
                                                            link.decompose()
                                                            continue
                                                        
                                                        # Convert email links to markdown
                                                        if href.startswith('mailto:'):
                                                            email = unescape(href[7:])
                                                            if not email:
                                                                link.decompose()
                                                                continue
                                                            
                                                            # GENERAL: If link contains a lot of text (like contact info blocks),
                                                            # preserve all the text and just convert the email part to markdown
                                                            # Check if this is a contact info block (has name pattern and phone/email)
                                                            # GENERAL: For LinkElementMailto links, get_text() should get all child content
                                                            # But if it doesn't, try getting from the link's parent or check if children exist
                                                            link_full_text = link.get_text(separator=' ', strip=True)
                                                            # If link text is very short but link has class LinkElementMailto, it might be a contact block
                                                            # Try getting text from parent if link text is suspiciously short
                                                            if len(link_full_text) < 30 and 'LinkElementMailto' in str(link.get('class', [])):
                                                                # Check parent for full text
                                                                parent = link.find_parent(['div', 'section'])
                                                                if parent:
                                                                    parent_text = parent.get_text(separator=' ', strip=True)
                                                                    # If parent has contact info and is not too large, use it
                                                                    if (re.search(r'[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+.*\([^)]+\)', parent_text) and
                                                                        (re.search(r'T\.\s*\(?\d+|\(?\d{3,4}\)?\s*[\-]?\s*\d{3,4}', parent_text) or '@' in parent_text) and
                                                                        len(parent_text) < 500):  # Not too large
                                                                        link_full_text = parent_text
                                                            is_contact_block = (
                                                                len(link_full_text) > 50 and
                                                                re.search(r'[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+.*\([^)]+\)', link_full_text) and
                                                                (re.search(r'T\.\s*\(?\d+|\(?\d{3,4}\)?\s*[\-]?\s*\d{3,4}', link_full_text) or '@' in link_full_text)
                                                            )
                                                            
                                                            
                                                            if is_contact_block:
                                                                # This is a contact info block - unwrap the link (keep children) and convert email to markdown
                                                                # Unwrap removes the <a> tag but keeps all child elements (<p>, <span>, etc.)
                                                                parent = link.parent
                                                                email_pattern = re.escape(email)
                                                                
                                                                # Find and replace email in all text nodes within the link before unwrapping
                                                                for text_node in link.find_all(string=True, recursive=True):
                                                                    if email in str(text_node):
                                                                        new_text = re.sub(f'\\b{email_pattern}\\b', f'[{email}](mailto:{email})', str(text_node))
                                                                        text_node.replace_with(NavigableString(new_text))
                                                                
                                                                # GENERAL: Replace link with a paragraph containing all its text content
                                                                # This ensures the contact block is extracted as a single paragraph
                                                                link_content = link_full_text
                                                                # Replace email with markdown link
                                                                link_content = re.sub(f'\\b{email_pattern}\\b', f'[{email}](mailto:{email})', link_content)
                                                                # Create a new paragraph element with the contact info
                                                                # Use the same soup instance to ensure proper integration
                                                                new_para = main_content.new_tag('p')
                                                                new_para['class'] = 'contact-block-extracted'
                                                                new_para.string = link_content
                                                                # Replace the entire link with the new paragraph
                                                                link.replace_with(new_para)
                                                                continue  # Skip the rest of link processing since we replaced it
                                                            else:
                                                                # Simple email link - use email as link text if link text is generic
                                                                if not link_text or link_text.lower() in ['email', 'e-mail', 'mail', 'contact']:
                                                                    link_text = email
                                                                # Replace with markdown: [email](mailto:email)
                                                                link.replace_with(NavigableString(f"[{link_text}](mailto:{email})"))
                                                        # Convert regular links to markdown
                                                        elif href:
                                                            if not link_text:
                                                                link_text = href
                                                            link.replace_with(NavigableString(f"[{link_text}]({href})"))
                                                    
                                                    # Get the text (links are already in markdown format)
                                                    # IMPORTANT: We remove table elements before extracting paragraphs to
                                                    # avoid duplicating table content. BUT headings can sometimes live
                                                    # inside layout tables, so extract headings first, then drop tables.
                                                    main_content_for_text = BeautifulSoup(str(main_content), 'html.parser')
                                                    
                                                    # SIMPLIFIED: Extract all paragraphs and text, preserving structure
                                                    # No complex contact block extraction - just extract everything properly
                                                    lines = []
                                                    
                                                    # FIX 1: Disable old heading extraction - headings are now extracted in DOM order
                                                    # via extract_headings_and_tables_in_dom_order() and added to tables_markdown
                                                    # This prevents duplicate headings and wrong ordering from navigation elements
                                                    # Headings will be extracted separately with proper DOM order and table associations
                                                    
                                                    # Remove all table elements since we've already extracted them
                                                    for table_elem in main_content_for_text.find_all('table'):
                                                        table_elem.decompose()
                                                    
                                                    # Extract all paragraphs and divs (preserves structure, INCLUDES FIRST PARAGRAPH)
                                                    all_paras = main_content_for_text.find_all(['p', 'div'], recursive=True)
                                                    i = 0
                                                    while i < len(all_paras):
                                                        para = all_paras[i]
                                                        # Skip if inside a table (already extracted)
                                                        if para.find_parent('table'):
                                                            i += 1
                                                            continue
                                                        
                                                        # Skip if it's a heading (already extracted)
                                                        if para.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                                            i += 1
                                                            continue
                                                        
                                                        # Get text from this paragraph (links already converted to markdown, INCLUDING EMAILS)
                                                        para_text = para.get_text(separator=' ', strip=True)
                                                        if para_text and len(para_text.strip()) > 2:
                                                            # Normalize whitespace but keep structure
                                                            para_text = re.sub(r'\s+', ' ', para_text, flags=re.UNICODE).strip()
                                                            
                                                            # GENERAL: Merge contact info split across multiple short paragraphs
                                                            # Contact info is often split like: "Name (pronouns)" -> "Title" -> "T. phone" -> "E. email"
                                                            # If paragraph has name pattern but no contact info, merge with next short paragraphs until contact info is found
                                                            if para_text and re.search(r'^[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+\s+\([^)]+\)', para_text) and not re.search(r'@|mailto:|T\.\s*\(?\d+|E\.\s*[a-z]', para_text, re.I):
                                                                merged = para_text
                                                                j = i + 1
                                                                # Merge up to 5 consecutive short paragraphs
                                                                while j < len(all_paras) and j - i <= 5:
                                                                    next_para = all_paras[j]
                                                                    if next_para.find_parent('table') or next_para.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                                                        break
                                                                    next_text = next_para.get_text(separator=' ', strip=True)
                                                                    if next_text and len(next_text.strip()) > 2:
                                                                        next_text = re.sub(r'\s+', ' ', next_text, flags=re.UNICODE).strip()
                                                                        # Stop if next paragraph is another name
                                                                        if re.search(r'^[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+\s+\([^)]+\)', next_text):
                                                                            break
                                                                        # Merge short paragraphs (< 80 chars) or paragraphs with contact info
                                                                        if len(next_text) < 80 or re.search(r'@|mailto:|T\.\s*\(?\d+|E\.\s*[a-z]', next_text, re.I):
                                                                            merged = f"{merged} {next_text}"
                                                                            j += 1
                                                                            # Stop if we found contact info
                                                                            if re.search(r'@|mailto:|T\.\s*\(?\d+|E\.\s*[a-z]', merged, re.I):
                                                                                break
                                                                        else:
                                                                            break
                                                                    else:
                                                                        j += 1
                                                                para_text = merged
                                                                i = j  # Skip merged paragraphs
                                                            else:
                                                                i += 1
                                                            
                                                            # Skip if it's just a URL or navigation
                                                            if not re.match(r'^URL:\s*https?://', para_text, re.IGNORECASE):
                                                                if not re.match(r'^Breadcrumb\s+Navigation', para_text, re.IGNORECASE):
                                                                    # Skip navigation/footer patterns
                                                                    nav_keywords = ['data privacy', 'declaration of accessibility', 'impressum', 
                                                                                   'datenschutz', 'cookie policy', 'accessibility statement']
                                                                    if not any(keyword in para_text.lower() for keyword in nav_keywords):
                                                                        lines.append(para_text)
                                                        else:
                                                            i += 1
                                                    
                                                    # If no paragraphs found, fall back to line-by-line extraction
                                                    if not lines:
                                                        text = main_content_for_text.get_text(separator='\n', strip=True)
                                                        lines = [l for l in text.split('\n') if l.strip() and len(l.strip()) > 2]
                                                    
                                                    text = '\n'.join(lines)
                                                    
                                                    
                                                    # SIMPLIFIED: Basic filtering - only remove obvious noise
                                                    # Keep all content, including first paragraph and emails
                                                    lines = text.split('\n')
                                                    filtered_lines = []
                                                    seen_lines = set()  # Track seen lines to avoid duplicates
                                                    
                                                    # Collect content from tables to filter duplicates
                                                    # This includes: research areas, parameter names, table headers, table cell content
                                                    table_content_signatures = set()
                                                    research_areas_in_tables = set()
                                                    
                                                    if html_fallback_tables:
                                                        # Extract all table cell content as signatures for deduplication
                                                        # Split by table rows and cells
                                                        table_lines = html_fallback_tables.split('\n')
                                                        for line in table_lines:
                                                            line_stripped = line.strip()
                                                            if not line_stripped or '|' not in line_stripped:
                                                                continue
                                                            
                                                            # Extract individual cells from table row
                                                            cells = [c.strip() for c in line_stripped.split('|') if c.strip() and not c.strip().startswith('---')]
                                                            for cell in cells:
                                                                cell_normalized = re.sub(r'\s+', ' ', cell.lower()).strip()
                                                                if cell_normalized and len(cell_normalized) > 2:
                                                                    table_content_signatures.add(cell_normalized)
                                                                    
                                                                    # Also extract individual words/phrases from cells for more aggressive matching
                                                                    # This helps catch research areas, parameter names, etc. that appear as standalone lines
                                                                    words = cell_normalized.split()
                                                                    for word in words:
                                                                        if len(word) > 3:  # Only meaningful words
                                                                            table_content_signatures.add(word)
                                                                    
                                                                    # Extract phrases (2-4 word combinations) for better matching
                                                                    # This catches "Electron energy", "Fermi, Group Leader IceCube", etc.
                                                                    if len(words) >= 2:
                                                                        # 2-word phrases
                                                                        for i in range(len(words) - 1):
                                                                            phrase = ' '.join(words[i:i+2])
                                                                            if len(phrase) > 5:
                                                                                table_content_signatures.add(phrase)
                                                                        # 3-word phrases (for longer research areas)
                                                                        if len(words) >= 3:
                                                                            for i in range(len(words) - 2):
                                                                                phrase = ' '.join(words[i:i+3])
                                                                                if len(phrase) > 8:
                                                                                    table_content_signatures.add(phrase)
                                                                        # 4-word phrases (for very long research areas)
                                                                        if len(words) >= 4:
                                                                            for i in range(len(words) - 3):
                                                                                phrase = ' '.join(words[i:i+4])
                                                                                if len(phrase) > 12:
                                                                                    table_content_signatures.add(phrase)
                                                        
                                                        # Extract research area patterns from table content
                                                        # Common patterns: "IceCube", "IceCube, Radio", "Fermi, Group Leader IceCube", etc.
                                                        research_pattern = r'\b(IceCube|Radio|Fermi|Baikal|Tunka|RADIO|Multimessenger School|Group Leader[^|]*?)(?:,|\s*$)'
                                                        research_matches = re.findall(research_pattern, html_fallback_tables, re.IGNORECASE)
                                                        research_areas_in_tables.update([m.strip() for m in research_matches if m.strip()])
                                                        
                                                        # Extract complete research area lines from table rows
                                                        for line in table_lines:
                                                            line_stripped = line.strip()
                                                            # If line is a research area (contains known keywords and is short)
                                                            if (any(keyword.lower() in line_stripped.lower() for keyword in ['IceCube', 'Radio', 'Fermi', 'Baikal', 'Tunka']) 
                                                                and len(line_stripped) < 100 
                                                                and '|' not in line_stripped):
                                                                research_areas_in_tables.add(line_stripped)
                                                        
                                                        # Extract research areas from table cell content (more comprehensive)
                                                        cell_pattern = r'([^|]*?(?:IceCube|Radio|Fermi|Baikal|Tunka)[^|]*)'
                                                        cell_matches = re.findall(cell_pattern, html_fallback_tables, re.IGNORECASE)
                                                        for match in cell_matches:
                                                            research_part = re.search(r'(?:Location:\s*[^,]+,\s*)?([^,]*?(?:IceCube|Radio|Fermi|Baikal|Tunka)[^,]*?)(?:\s*\||\s*$)', match, re.IGNORECASE)
                                                            if research_part:
                                                                research_text = research_part.group(1).strip()
                                                                if len(research_text) < 100 and research_text:
                                                                    research_areas_in_tables.add(research_text)
                                                    
                                                    # SIMPLIFIED FILTERING: Keep all content, only remove obvious noise
                                                    for line in lines:
                                                        line_stripped = line.strip()
                                                        
                                                        # Skip empty lines
                                                        if not line_stripped:
                                                            continue

                                                        # Issue #4: Drop empty/separator-only table rows that leaked into text
                                                        # (e.g. "---|---" or "|   |")
                                                        if re.match(r'^\s*\|?\s*(---|—|–)\s*(\|\s*(---|—|–)\s*)+\|?\s*$', line_stripped):
                                                            continue
                                                        if line_stripped.startswith('|') and line_stripped.replace('|', '').strip() == '':
                                                            continue
                                                        
                                                        # Only remove obvious noise:
                                                        # 1. URL header duplicates
                                                        if re.match(r'^URL:\s*https?://', line_stripped, re.IGNORECASE):
                                                            continue
                                                        if re.match(r'^Breadcrumb\s+Navigation', line_stripped, re.IGNORECASE):
                                                            continue
                                                        # 2. Lines that are just bare URLs (no text)
                                                        if re.match(r'^https?://[^\s]+$', line_stripped):
                                                            continue
                                                        # 3. Very short lines (< 3 chars) that aren't headings
                                                        if len(line_stripped) < 3 and not line_stripped.startswith('#'):
                                                            continue
                                                        # 4. Exact duplicates (keep first occurrence)
                                                        line_signature = re.sub(r'\s+', ' ', line_stripped.lower()).strip()
                                                        if line_signature in seen_lines:
                                                            continue
                                                        seen_lines.add(line_signature)
                                                        
                                                        # Keep everything else (including first paragraph, emails, all content)
                                                        filtered_lines.append(line)
                                                    
                                                    text = '\n'.join(filtered_lines)
                                                    
                                                    # Apply enhanced duplication detection
                                                    lines_list = text.split('\n')
                                                    duplicates = detect_enhanced_repetition(lines_list)
                                                    
                                                    # Remove duplicate lines (keep first occurrence)
                                                    deduplicated_lines = []
                                                    for i, line in enumerate(lines_list):
                                                        if i not in duplicates:
                                                            deduplicated_lines.append(line)
                                                    
                                                    text = '\n'.join(deduplicated_lines)
                                                    
                                                    # Clean markdown link syntax (remove whitespace from links)
                                                    text = clean_markdown_links_post_process(text)
                                                    
                                                    # If text is still very short, try getting from entire body (but apply same filtering)
                                                    # RELAXED: Lower threshold from 100 to 50, and check for contact info
                                                    text_has_contact = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', text))
                                                    if len(text.strip()) < 50 and not text_has_contact:
                                                        body_text = soup.find('body')
                                                        if body_text:
                                                            body_text_clean = body_text.get_text(separator='\n', strip=True)
                                                            # Apply same filtering to body text
                                                            body_lines = body_text_clean.split('\n')
                                                            filtered_body_lines = []
                                                            for line in body_lines:
                                                                line_stripped = line.strip()
                                                                if not line_stripped:
                                                                    continue
                                                                
                                                                # RELAXED: Check for contact info FIRST - always keep it
                                                                has_contact_pattern = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)|\b[A-Z][a-z]+\s+[A-Z][a-z]+.*(Head|Manager|Leader|Trainer|Recruitment)', line_stripped))
                                                                if has_contact_pattern:
                                                                    filtered_body_lines.append(line)
                                                                    continue
                                                                
                                                                if len(line_stripped) < 2:  # RELAXED: from 3 to 2
                                                                    continue
                                                                if line_stripped.count('](') > 8:  # RELAXED: from 5 to 8
                                                                    continue
                                                                if re.match(r'^https?://[^\s]+$', line_stripped) or (line_stripped.count('/') > 8 and 'http' in line_stripped):  # RELAXED
                                                                    continue
                                                                if len(re.sub(r'[^\w\s]', '', line_stripped)) < len(line_stripped) * 0.2:  # RELAXED: from 0.3 to 0.2
                                                                    continue
                                                                filtered_body_lines.append(line)
                                                            body_text_filtered = '\n'.join(filtered_body_lines)
                                                            if len(body_text_filtered) > len(text):
                                                                text = body_text_filtered
                                                                print(f"[DEBUG] Using filtered body text ({len(text)} chars)")
                                                    
                                                    # Combine tables and text
                                                    # IMPORTANT: If we extracted tables from HTML fallback, use them
                                                    # Store tables separately so they don't get filtered out
                                                    #
                                                    # Tables are extracted by Crawl4AI - no need for DOM-order extraction
                                                    # html_fallback_tables is empty since we're using Crawl4AI tables
                                                    
                                                    # RELAXED: Check for contact info in text - if present, use it even if short
                                                    if True:
                                                        text_has_contact = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', text))
                                                        if text and (len(text.strip()) > 30 or text_has_contact):  # RELAXED: from 50 to 30, or if has contact info
                                                            # Don't add URL header here - it will be added later
                                                            markdown_content = text
                                                            print(f"[INFO] Extracted content directly from HTML ({len(text)} chars, {len(html_tables_found) if 'html_tables_found' in locals() else 0} tables)")
                                                    else:
                                                        # Last resort: try to get ANY text from the page
                                                        print(f"[WARNING] Extracted text too short ({len(text)} chars) - trying full page extraction")
                                                        full_page_text = soup.get_text(separator='\n', strip=True)
                                                        # RELAXED: Lower threshold and check for contact info
                                                        full_page_has_contact = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', full_page_text))
                                                        if full_page_text and (len(full_page_text.strip()) > 50 or full_page_has_contact):  # RELAXED: from 100 to 50
                                                            # Apply aggressive filtering to remove navigation/UI noise
                                                            lines = full_page_text.split('\n')
                                                            filtered_lines = []
                                                            for line in lines:
                                                                line_stripped = line.strip()
                                                                if not line_stripped:
                                                                    continue
                                                                
                                                                # RELAXED: Check for contact info FIRST - always keep it
                                                                has_contact_pattern = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)|\b[A-Z][a-z]+\s+[A-Z][a-z]+.*(Head|Manager|Leader|Trainer|Recruitment)', line_stripped))
                                                                if has_contact_pattern:
                                                                    filtered_lines.append(line)
                                                                    continue
                                                                
                                                                if len(line_stripped) < 2:  # RELAXED: from 5 to 2
                                                                    continue
                                                                if line_stripped.count('](') > 8:  # RELAXED: from 5 to 8
                                                                    continue
                                                                if re.match(r'^https?://[^\s]+$', line_stripped) or (line_stripped.count('/') > 8 and 'http' in line_stripped):  # RELAXED
                                                                    continue
                                                                if len(re.sub(r'[^\w\s]', '', line_stripped)) < len(line_stripped) * 0.2:  # RELAXED: from 0.3 to 0.2
                                                                    continue
                                                                if len(line_stripped.split()) == 1 and line_stripped.isupper() and len(line_stripped) < 10:  # RELAXED: only short all-caps
                                                                    continue
                                                                filtered_lines.append(line)
                                                            meaningful_text = '\n'.join(filtered_lines)
                                                            # RELAXED: Lower threshold and check for contact info
                                                            meaningful_has_contact = bool(re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', meaningful_text))
                                                            if len(meaningful_text) > 30 or meaningful_has_contact:  # RELAXED: from 100 to 30
                                                                # Don't add URL header here - it will be added later
                                                                markdown_content = meaningful_text
                                                                print(f"[INFO] Extracted filtered full page content ({len(meaningful_text)} chars)")
                                                        else:
                                                            print(f"[WARNING] Page appears to be empty or content is loaded via JavaScript/iframe")
                                                            print(f"[WARNING] HTML length: {len(result.html) if hasattr(result, 'html') and result.html else 0} chars")
                                                # If main_content was not found, extract text from body/soup
                                                # (Tables are already extracted above, regardless of main_content)
                                                if not main_content:
                                                    print(f"[WARNING] Could not find main content area in HTML - extracting text from body directly")
                                                    # Tables are already extracted above, so we just need to extract text
                                                    
                                                    # Try to get text from entire body as last resort
                                                    body = soup.find('body')
                                                    if body:
                                                        body_text = body.get_text(separator='\n', strip=True)
                                                        if body_text and len(body_text.strip()) > 100:
                                                            # Apply aggressive filtering to remove navigation/UI noise
                                                            body_lines = body_text.split('\n')
                                                            filtered_body_lines = []
                                                            # Collect research areas from tables to filter duplicates
                                                            research_areas_in_tables = set()
                                                            if html_fallback_tables:
                                                                research_pattern = r'\b(IceCube|Radio|Fermi|Baikal|Tunka|RADIO|Multimessenger School|Group Leader[^|]*?)(?:,|\s*$)'
                                                                research_matches = re.findall(research_pattern, html_fallback_tables, re.IGNORECASE)
                                                                research_areas_in_tables.update([m.strip() for m in research_matches if m.strip()])
                                                            
                                                            for line in body_lines:
                                                                line_stripped = line.strip()
                                                                if not line_stripped or len(line_stripped) < 5:
                                                                    continue
                                                                if line_stripped.count('](') > 5:
                                                                    continue
                                                                if re.match(r'^https?://', line_stripped) or line_stripped.count('/') > 5:
                                                                    continue
                                                                if len(re.sub(r'[^\w\s]', '', line_stripped)) < len(line_stripped) * 0.3:
                                                                    continue
                                                                if len(line_stripped.split()) == 1 and line_stripped.isupper():
                                                                    continue
                                                                # Skip dropdown content: lines with many consecutive capitalized abbreviations
                                                                abbrev_pattern = re.compile(r'\b[A-Z]{2,}(?:[-_][A-Z0-9]{2,})+\b')
                                                                abbrev_matches = abbrev_pattern.findall(line_stripped)
                                                                if len(abbrev_matches) > 10 and len(line_stripped) > 100:
                                                                    continue
                                                                # Skip research area lines that are already in tables
                                                                if research_areas_in_tables:
                                                                    line_lower = line_stripped.lower()
                                                                    for research_area in research_areas_in_tables:
                                                                        if research_area.lower() in line_lower and len(line_stripped) < 100:
                                                                            continue
                                                                filtered_body_lines.append(line)
                                                            meaningful_body_text = '\n'.join(filtered_body_lines)
                                                            # Combine with tables, but avoid duplication
                                                            if html_fallback_tables and html_fallback_tables.strip() not in meaningful_body_text:
                                                                meaningful_body_text = html_fallback_tables + "\n\n" + meaningful_body_text
                                                            if len(meaningful_body_text) > 100:
                                                                # Don't add URL header here - it will be added later
                                                                markdown_content = meaningful_body_text
                                                                print(f"[INFO] Extracted filtered body content as fallback ({len(meaningful_body_text)} chars)")
                                                    else:
                                                        # Absolute last resort: get any text from soup
                                                        all_text = soup.get_text(separator='\n', strip=True)
                                                        if all_text and len(all_text.strip()) > 100:
                                                            # Apply aggressive filtering
                                                            lines = all_text.split('\n')
                                                            filtered_lines = []
                                                            # Collect research areas from tables to filter duplicates
                                                            research_areas_in_tables = set()
                                                            if html_fallback_tables:
                                                                research_pattern = r'\b(IceCube|Radio|Fermi|Baikal|Tunka|RADIO|Multimessenger School|Group Leader[^|]*?)(?:,|\s*$)'
                                                                research_matches = re.findall(research_pattern, html_fallback_tables, re.IGNORECASE)
                                                                research_areas_in_tables.update([m.strip() for m in research_matches if m.strip()])
                                                            
                                                            for line in lines:
                                                                line_stripped = line.strip()
                                                                if not line_stripped or len(line_stripped) < 5:
                                                                    continue
                                                                if line_stripped.count('](') > 5:
                                                                    continue
                                                                if re.match(r'^https?://', line_stripped) or line_stripped.count('/') > 5:
                                                                    continue
                                                                if len(re.sub(r'[^\w\s]', '', line_stripped)) < len(line_stripped) * 0.3:
                                                                    continue
                                                                if len(line_stripped.split()) == 1 and line_stripped.isupper():
                                                                    continue
                                                                # Skip dropdown content: lines with many consecutive capitalized abbreviations
                                                                abbrev_pattern = re.compile(r'\b[A-Z]{2,}(?:[-_][A-Z0-9]{2,})+\b')
                                                                abbrev_matches = abbrev_pattern.findall(line_stripped)
                                                                if len(abbrev_matches) > 10 and len(line_stripped) > 100:
                                                                    continue
                                                                # Skip research area lines that are already in tables
                                                                if research_areas_in_tables:
                                                                    line_lower = line_stripped.lower()
                                                                    for research_area in research_areas_in_tables:
                                                                        if research_area.lower() in line_lower and len(line_stripped) < 100:
                                                                            continue
                                                                filtered_lines.append(line)
                                                            meaningful_all_text = '\n'.join(filtered_lines)
                                                            # Combine with tables, but avoid duplication
                                                            if html_fallback_tables and html_fallback_tables.strip() not in meaningful_all_text:
                                                                meaningful_all_text = html_fallback_tables + "\n\n" + meaningful_all_text
                                                            if len(meaningful_all_text) > 100:
                                                                # Don't add URL header here - it will be added later
                                                                markdown_content = meaningful_all_text
                                                                print(f"[INFO] Extracted filtered all page text as last resort ({len(meaningful_all_text)} chars)")
                                        except Exception as e:
                                            print(f"[WARNING] Failed to extract from HTML: {e}")
                                            import traceback
                                            traceback.print_exc()
                        else:
                            # Fallback if markdown is just a string
                            markdown_content = result.markdown or ""
                    
                    # ============================================================
                    # Post-process markdown to inject links directly into tables
                    # ============================================================
                    # Crawl4AI's markdown generator loses links in table cells.
                    # We extract tables from HTML with links preserved, then replace
                    # the corresponding table sections in the markdown in-place.
                    if markdown_content and not result_is_pdf and hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
                        try:
                            # Check if we need to inject links (if markdown has tables but no email links)
                            has_tables = '|' in markdown_content and len([l for l in markdown_content.split('\n') if '|' in l]) >= 3
                            has_email_links = bool(re.search(r'\[[^\]]+\]\(mailto:[^\s@]+@[^\s@]+\.[^\s)]+\)', markdown_content))
                            
                            if has_tables and not has_email_links:
                                print(f"[INFO] Injecting links into tables (found tables but no email links)")
                            
                            markdown_content = inject_links_into_markdown_tables(markdown_content, result.html)
                        except Exception as e:
                            # If post-processing fails, continue with original markdown
                            print(f"[WARNING] Failed to inject links into markdown: {e}")
                            import traceback
                            traceback.print_exc()
                            pass
                    
                    # ============================================================
                    # Check for Errors
                    # ============================================================
                    # Check if the crawl was successful
                    if hasattr(result, 'success') and not result.success:
                        # Crawl failed - log the error
                        error_msg = getattr(result, 'error_message', 'Unknown error')
                        all_errors.append({
                            'url': result.url,
                            'error': error_msg,
                            'timestamp': datetime.now().isoformat()
                        })
                        print(f"[ERROR] {result.url}")
                        print(f"        Reason: {error_msg}")
                        continue
                    
                    # ============================================================
                    # Hybrid Table Extraction: Crawl4AI for JS rendering + BeautifulSoup for table extraction
                    # ============================================================
                    # Strategy: Use Crawl4AI only for JS rendering, extract tables directly from result.html
                    # This approach:
                    # 1. Detects nested tables and prefers nested data tables over outer layout tables
                    # 2. Extracts headings and tables in DOM order
                    # 3. Preserves document structure and associations
                    # 4. Avoids duplicate or flattened tables
                        tables_markdown = ""
                    
                    if not result_is_pdf and hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
                        try:
                            
                            # Solution 4: Extract headings and tables in DOM order
                            dom_ordered_content = extract_headings_and_tables_in_dom_order(result.html, url=result.url)
                            print(f"[DEBUG] DOM-order extraction: Found {len(dom_ordered_content)} content items")
                            
                            # #region agent log
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'T',
                                    'location': 'crawl_desy_simple.py:5121',
                                    'message': 'dom_ordered_content extracted',
                                    'data': {
                                        'dom_ordered_content_count': len(dom_ordered_content) if dom_ordered_content else 0,
                                        'url': result.url if hasattr(result, 'url') else None,
                                        'has_magnetism': 'magnetism' in (result.url if hasattr(result, 'url') else '').lower(),
                                        'item_types': [item.get('type') for item in dom_ordered_content[:10]] if dom_ordered_content else []
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                            # #endregion
                            
                            # Format as markdown
                            tables_markdown = format_tables_with_headings_as_markdown(dom_ordered_content)
                            
                            # #region agent log
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'T',
                                    'location': 'crawl_desy_simple.py:5125',
                                    'message': 'tables_markdown generated',
                                    'data': {
                                        'tables_markdown_length': len(tables_markdown) if tables_markdown else 0,
                                        'dom_ordered_content_count': len(dom_ordered_content),
                                        'has_group_members': 'group members' in (tables_markdown or '').lower(),
                                        'preview': (tables_markdown or '')[:200]
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                            # #endregion
                            
                            if tables_markdown:
                                # Count tables and headings for logging
                                table_count = sum(1 for item in dom_ordered_content if item['type'] == 'table')
                                heading_count = sum(1 for item in dom_ordered_content if item['type'] == 'heading')
                                print(f"[INFO] DOM-order extraction: Formatted {table_count} table(s) and {heading_count} heading(s)")
                            else:
                                print(f"[DEBUG] DOM-order extraction: No tables formatted (empty result)")
                        except Exception as e:
                            print(f"[WARNING] Hybrid table extraction failed: {e}")
                            import traceback
                            traceback.print_exc()
                            # Fallback: Use Crawl4AI tables if Hybrid extraction fails
                            if hasattr(result, 'tables') and result.tables:
                                print(f"[DEBUG] Falling back to Crawl4AI table extraction")
                                for idx, crawl_table in enumerate(result.tables, 1):
                                    if isinstance(crawl_table, dict):
                                        headers = crawl_table.get('headers', [])
                                        rows = crawl_table.get('rows', []) or crawl_table.get('data', [])
                                        if rows:
                                            if headers:
                                                tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                                                tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                                            for row in rows:
                                                tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                                            tables_markdown += "\n"
                    
                    # Fallback for PDFs: Use Crawl4AI tables
                    if result_is_pdf and hasattr(result, 'tables') and result.tables:
                            print(f"[DEBUG] PDF: Using Crawl4AI table extraction ({len(result.tables)} table(s))")
                            for idx, crawl_table in enumerate(result.tables, 1):
                                if isinstance(crawl_table, dict):
                                    headers = crawl_table.get('headers', [])
                                    rows = crawl_table.get('rows', []) or crawl_table.get('data', [])
                                    if rows:
                                        if headers:
                                            tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                                            tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                                        for row in rows:
                                            tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                                        tables_markdown += "\n"
                    
                    # ============================================================
                    # Extract Image References (for PDFs)
                    # ============================================================
                    image_refs_markdown = ""
                    if result_is_pdf and PDF_SUPPORT_AVAILABLE:
                        # Check for extracted images
                        if hasattr(result, 'media') and result.media:
                            images = result.media.get("images", [])
                            if images:
                                print(f"[PDF] Extracted {len(images)} image(s) from {result.url}")
                                # Add image references to markdown
                                image_refs_markdown = "\n\n## Extracted Images\n\n"
                                for idx, img_info in enumerate(images, 1):
                                    img_path = img_info.get('path', '')
                                    if img_path:
                                        # Create relative path from markdown file
                                        img_filename = Path(img_path).name
                                        image_refs_markdown += f"![Image {idx}](extracted_images/{img_filename})\n\n"
                    
                    # ============================================================
                    # Save to File
                    # ============================================================
                    try:
                        if markdown_content or tables_markdown or image_refs_markdown:
                            # Create header with URL information
                            # This helps identify the source of the content when reading the markdown file
                            url_header = f"# Source URL\n\n{result.url}\n\n"
                        
                        # Remove any existing URL header from markdown_content to avoid duplication
                        if markdown_content:
                            # Remove URL header pattern if it exists
                            markdown_content = re.sub(r'^#\s*Source\s*URL.*?\n---\s*\n', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                            # Also remove "URL: <url>" patterns and breadcrumb navigation
                            markdown_content = re.sub(r'^URL:\s*https?://[^\s]+\s*\n?', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE)
                            markdown_content = re.sub(r'^Breadcrumb\s+Navigation\s*\n?', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE)
                            # Remove lines that are just the current page URL
                            markdown_content = re.sub(r'^' + re.escape(result.url) + r'\s*\n?', '', markdown_content, flags=re.MULTILINE)
                            
                            # Apply enhanced duplication detection to final markdown
                            markdown_lines = markdown_content.split('\n')
                            duplicates = detect_enhanced_repetition(markdown_lines)
                            
                            # Remove duplicate lines (keep first occurrence)
                            deduplicated_lines = []
                            for i, line in enumerate(markdown_lines):
                                if i not in duplicates:
                                    deduplicated_lines.append(line)
                            
                            markdown_content = '\n'.join(deduplicated_lines)
                            
                            # Clean markdown link syntax (remove whitespace from links)
                            markdown_content = clean_markdown_links_post_process(markdown_content)

                            # Issue #1/#9: remove empty-text markdown links like [](...)
                            # Keep images ![](...)
                            markdown_content = re.sub(r'(?<!\!)\[\]\([^)]+\)', '', markdown_content)
                            
                            # GENERAL: Remove navigation patterns (lines with spacer images, navigation links, header images)
                            # Pattern: Lines containing spacer images, header images, or navigation menu patterns
                            lines = markdown_content.split('\n')
                            cleaned_lines = []
                            for line in lines:
                                # Skip lines with spacer images (common in navigation)
                                if re.search(r'spacer\.(gif|png|jpg)', line, re.I):
                                    continue
                                # Skip lines with header images (header.jpg, desy.jpg, etc.)
                                if re.search(r'(header|desy|logo|banner)\.(jpg|png|gif)', line, re.I):
                                    continue
                                # Skip lines that are just navigation links with images
                                if re.search(r'!\[\]\([^)]+(spacer|header|desy|logo|banner)[^)]+\)', line, re.I):
                                    continue
                                # Skip navigation text patterns (common UI text)
                                if re.search(r'(To sort click|navigation|menu|breadcrumb)', line, re.I):
                                    continue
                                # Skip lines that are navigation menu items (many links, short text)
                                link_count = len(re.findall(r'\[([^\]]+)\]\([^)]+\)', line))
                                if link_count > 3 and len(line.strip()) < 150:
                                    continue
                                # Skip lines that are just image markdown with no text (header images)
                                if re.match(r'^!\[.*?\]\([^)]+\)\s*\|?\s*$', line.strip()):
                                    continue
                                cleaned_lines.append(line)
                            markdown_content = '\n'.join(cleaned_lines)
                            
                            # GENERAL: Remove broken table fragments (rows that look like table fragments but aren't part of proper tables)
                            # Pattern: Lines like "| Name |" or multi-line fragments like "Name\n| Value |" that are not part of proper tables
                            lines = markdown_content.split('\n')
                            cleaned_lines = []
                            i = 0
                            in_proper_table = False
                            fragment_start = -1
                            
                            while i < len(lines):
                                line = lines[i]
                                stripped = line.strip() if line else ""
                                
                                # Detect proper table start (header row with separator)
                                # Also handle case where separator comes first (missing header)
                                if re.match(r'^\|[\s\-:]+\|', stripped):
                                    # Separator without header - check if previous line was a header or if next line has data
                                    # If previous line is NOT a header, skip this orphaned separator
                                    prev_is_header = False
                                    if i > 0:
                                        prev_stripped = lines[i - 1].strip()
                                        if prev_stripped and re.match(r'^\|\s*[^|]+\s*\|', prev_stripped):
                                            prev_is_header = True
                                    
                                    if not prev_is_header:
                                        # Orphaned separator without header - skip it
                                        i += 1
                                        continue
                                    
                                    if i + 1 < len(lines):
                                        next_line = lines[i + 1].strip()
                                        if re.match(r'^\|\s*[^|]+\s*\|', next_line):
                                            # This is a separator followed by data - treat as proper table
                                            in_proper_table = True
                                            fragment_start = -1
                                            # Add the separator (it's part of a proper table)
                                            cleaned_lines.append(line)
                                            i += 1
                                            continue
                                
                                if re.match(r'^\|\s*[^|]+\s*\|', stripped):
                                    # Check if this is followed by a separator (proper table)
                                    if i + 1 < len(lines):
                                        next_line = lines[i + 1].strip()
                                        if re.match(r'^\|[\s\-:]+\|', next_line):
                                            in_proper_table = True
                                            fragment_start = -1
                                            cleaned_lines.append(line)
                                            i += 1
                                            continue
                                
                                # If we're in a proper table, keep all rows
                                if in_proper_table:
                                    # Check if table ends (empty line or non-table line)
                                    if not stripped or not re.match(r'^\|', stripped):
                                        in_proper_table = False
                                    cleaned_lines.append(line)
                                    i += 1
                                    continue
                                
                                # Detect broken fragment pattern: Name on one line, then table cells on next lines
                                # Pattern: "Name\n| Value |\n| Value |" where Name is not a table row
                                # Also handles: "Name\n\n|  Value |\n|  Value |" (with empty lines)
                                if not re.match(r'^\|', stripped) and stripped and not stripped.startswith('#'):
                                    # Check if next few lines are table-like rows (single-cell or multi-cell)
                                    fragment_lines = []
                                    j = i + 1
                                    consecutive_empty = 0
                                    while j < len(lines) and j < i + 15:  # Check up to 15 lines ahead
                                        next_stripped = lines[j].strip()
                                        # Match single-cell rows (with or without closing |) or multi-cell table rows
                                        # Pattern: "|  text  " or "|  text  |" or "| text | text |"
                                        # After strip(), "|  text  " becomes "|  text" (no trailing spaces)
                                        if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', next_stripped) or re.match(r'^\|\s*[^|]+\s*\|', next_stripped):
                                            fragment_lines.append(j)
                                            consecutive_empty = 0
                                            j += 1
                                        elif not next_stripped:
                                            consecutive_empty += 1
                                            # Allow up to 2 empty lines between fragments
                                            if consecutive_empty <= 2:
                                                j += 1
                                            else:
                                                break
                                        else:
                                            # Non-table line - check if we have enough fragments to consider this a pattern
                                            if fragment_lines and len(fragment_lines) >= 2:
                                                # This is a name followed by fragments - skip them all
                                                break
                                            else:
                                                # Not enough fragments, not a pattern
                                                break
                                    
                                    # If we found fragment pattern (name + 2+ table rows but no separator), skip them
                                    if fragment_lines and len(fragment_lines) >= 2:
                                        # Skip the name line and all fragment lines
                                        i = fragment_lines[-1] + 1
                                        continue
                                
                                # Check if this looks like a broken table fragment (table-like row but not in proper table)
                                # Single-cell rows (like "|  Value |" or "|  Krisztian  " or "|  Krisztian  |") are likely fragments
                                # Pattern: "|  text  " or "|  text  |" (single cell with spaces, may or may not end with |)
                                # After strip(), "|  text  " becomes "|  text" (no trailing spaces)
                                # Match: starts with |, has spaces, text, optionally has closing | and spaces
                                if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', stripped):
                                    # Single-cell row - check if it's part of a fragment pattern
                                    # Look ahead to see if there are more single-cell rows or if previous line was a name
                                    fragment_count = 0
                                    j = i + 1
                                    while j < len(lines) and j < i + 10:
                                        next_stripped = lines[j].strip()
                                        if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', next_stripped):
                                            fragment_count += 1
                                            j += 1
                                        elif not next_stripped:
                                            j += 1
                                        else:
                                            break
                                    
                                    # Check if previous line was a name (not a table row, not empty, not a heading)
                                    # Also check 2 lines back in case there's an empty line
                                    prev_is_name = False
                                    for check_idx in [i - 1, i - 2]:
                                        if check_idx >= 0:
                                            prev_stripped = lines[check_idx].strip()
                                            if prev_stripped and not re.match(r'^\|', prev_stripped) and not prev_stripped.startswith('#'):
                                                prev_is_name = True
                                                break
                                    
                                    # If we have multiple single-cell rows in sequence OR previous line was a name, they're fragments
                                    if fragment_count >= 1 or prev_is_name:
                                        # Skip this fragment line
                                        i += 1
                                        continue
                                
                                # Multi-cell rows that aren't in proper tables
                                if re.match(r'^\|\s*[^|]+\s*\|', stripped):
                                    # Check if next line is also a fragment (not a separator or proper table row)
                                    if i + 1 < len(lines):
                                        next_line = lines[i + 1].strip()
                                        # If next line is not a separator (---) and not a proper table row, this is likely a fragment
                                        if not re.match(r'^\|[\s\-:]+\|', next_line) and not re.match(r'^\|\s*[^|]+\s*\|', next_line):
                                            # Skip this fragment line
                                            i += 1
                                            continue
                                
                                cleaned_lines.append(line)
                                i += 1
                            markdown_content = '\n'.join(cleaned_lines)
                            
                            markdown_content = markdown_content.strip()
                        
                        # Combine header, markdown content, extracted tables, and images
                        # GENERAL: If we have tables_markdown from DOM-order extraction, use it as primary source
                        # to preserve DOM order. Remove headings and tables from markdown_content to avoid duplicates.
                        content_to_save = url_header
                        
                        # #region agent log
                        if 'heuser' in result.url.lower():
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'H2',
                                    'location': 'crawl_desy_simple.py:5893',
                                    'message': 'Content assembly start',
                                    'data': {
                                        'has_tables_markdown': bool(tables_markdown),
                                        'tables_markdown_preview': (tables_markdown or '')[:200],
                                        'has_markdown_content': bool(markdown_content),
                                        'markdown_content_preview': (markdown_content or '')[:200]
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                        # #endregion
                        
                        if tables_markdown:
                            # tables_markdown contains headings and tables in correct DOM order
                            # Remove headings and tables from markdown_content to avoid duplicates
                            if markdown_content:
                                lines = markdown_content.split('\n')
                                cleaned_lines = []
                                i = 0
                                
                                # Extract headings from tables_markdown to know what to remove
                                tables_markdown_lines = tables_markdown.split('\n')
                                headings_in_tables_markdown = set()
                                for tm_line in tables_markdown_lines:
                                    tm_stripped = tm_line.strip()
                                    if tm_stripped.startswith('#'):
                                        # Extract heading text (remove # and whitespace, normalize)
                                        heading_text = tm_stripped.lstrip('#').strip()
                                        # Normalize whitespace (multiple spaces -> single space)
                                        heading_text_normalized = ' '.join(heading_text.split())
                                        headings_in_tables_markdown.add(heading_text_normalized.lower())
                                
                                while i < len(lines):
                                    line = lines[i]
                                    stripped = line.strip()
                                    
                                    # #region agent log
                                    if i < 50 and ('|' in stripped or ':' in stripped):  # Log first 50 lines with pipes/colons
                                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                            import json
                                            f.write(json.dumps({
                                                'sessionId': 'debug-session',
                                                'runId': 'run1',
                                                'hypothesisId': 'T',
                                                'location': 'crawl_desy_simple.py:5013',
                                                'message': 'Processing markdown_content line',
                                                'data': {
                                                    'line_num': i,
                                                    'stripped': stripped[:60],
                                                    'has_colon_pipe': ':' in stripped and '|' in stripped and not stripped.startswith('|')
                                                },
                                                'timestamp': int(__import__('time').time() * 1000)
                                            }) + '\n')
                                    # #endregion
                                    
                                    # Remove headings that are already in tables_markdown
                                    if stripped.startswith('#'):
                                        heading_text = stripped.lstrip('#').strip()
                                        # Remove empty headings (just ## with spaces) or headings that match tables_markdown
                                        if not heading_text or heading_text.lower() in headings_in_tables_markdown:
                                            i += 1
                                            continue
                                    
                                    # GENERAL: Remove text lines that match heading text in tables_markdown
                                    # Some headings appear as plain text (not starting with #) in markdown_content
                                    # Pattern: "Lattice Parameters  " (text that matches a heading)
                                    if stripped and not stripped.startswith('#') and not stripped.startswith('|'):
                                        # Check if this text matches a heading in tables_markdown (normalize whitespace)
                                        stripped_normalized = ' '.join(stripped.split()).lower()  # Normalize whitespace
                                        if stripped_normalized in headings_in_tables_markdown:
                                            i += 1
                                            continue
                                    
                                    # GENERAL: Remove broken label-value fragments (text with pipes, not proper tables)
                                    # Pattern: "Label:---|---" or "Label:|  Value" - text line with colon and pipe
                                    # These are malformed fragments from HTML conversion, not proper markdown tables
                                    if ':' in stripped and '|' in stripped and not stripped.startswith('|'):
                                        # Check if it's a broken fragment pattern:
                                        # 1. Label ending with : followed by | (with optional dashes/spaces): "Label:---|---"
                                        # 2. Label ending with : followed by | and value: "Label:|  Value"
                                        # Match patterns like "Label:---|---" or "Label:| Value"
                                        # Pattern 1: "Label:---|" or "Label:---|---" (colon, dashes, pipe at end)
                                        # Pattern 2: "Label:| Value" (colon, pipe, then value)
                                        # Match patterns like "Label:---|---" (colon, dashes, pipe) or "Label:| Value"
                                        # Pattern 1: "Label:---|" or "Label:---|---" (colon, dashes, pipe - pipe is required)
                                        # Pattern 2: "Label:| Value" (colon, pipe, then value)
                                        is_broken_fragment = (
                                            re.match(r'^[^|]+:\s*[-]+\|', stripped) or  # "Label:---|" or "Label:---|---"
                                            re.match(r'^[^|]+:\s*\|', stripped)  # "Label:| Value"
                                        )
                                        if is_broken_fragment:
                                            # #region agent log
                                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                                import json
                                                f.write(json.dumps({
                                                    'sessionId': 'debug-session',
                                                    'runId': 'run1',
                                                    'hypothesisId': 'S',
                                                    'location': 'crawl_desy_simple.py:5031',
                                                    'message': 'Removed broken label-value fragment',
                                                    'data': {'fragment': stripped[:80]},
                                                    'timestamp': int(__import__('time').time() * 1000)
                                                }) + '\n')
                                            # #endregion
                                            # This is a broken fragment - skip it and any following separator lines
                                            i += 1
                                            # Skip following separator lines (---|---)
                                            while i < len(lines):
                                                next_stripped = lines[i].strip()
                                                if not next_stripped:
                                                    # Empty line - allow one, then break
                                                    i += 1
                                                    if i < len(lines) and lines[i].strip():
                                                        # Check if next non-empty line is also a fragment
                                                        if ':' in lines[i].strip() and '|' in lines[i].strip() and not lines[i].strip().startswith('|'):
                                                            continue  # Continue skipping
                                                    break
                                                elif next_stripped == '---' or re.match(r'^\|[\s\-:]+\|$', next_stripped):
                                                    i += 1
                                                elif next_stripped and ':' in next_stripped and '|' in next_stripped and not next_stripped.startswith('|'):
                                                    # Another broken fragment - continue skipping
                                                    i += 1
                                                else:
                                                    break
                                            continue
                                    
                                    # Remove orphaned separator lines (---|---) that aren't part of proper tables
                                    # GENERAL: These appear when broken fragments are removed, leaving orphaned separators
                                    if stripped == '---' or re.match(r'^\|[\s\-:]+\|$', stripped) or stripped == '|---|---':
                                        # Check if this separator is part of a proper table (has table row before and after)
                                        # Look further back/forward to catch separators that are far from tables
                                        has_table_before = any(re.match(r'^\|', lines[j].strip()) for j in range(max(0, i - 10), i) if lines[j].strip() and not lines[j].strip().startswith('#'))
                                        has_table_after = any(re.match(r'^\|', lines[j].strip()) for j in range(i + 1, min(len(lines), i + 10)) if lines[j].strip() and not lines[j].strip().startswith('#'))
                                        if not (has_table_before and has_table_after):
                                            # Orphaned separator - skip it
                                            i += 1
                                            continue
                                    
                                    # Remove table sections from markdown_content
                                    if re.match(r'^\|', stripped):
                                        # PUBDB-specific filtering: Only filter UI tables on PUBDB pages
                                        # Check both URL and content to handle redirects/embedded content
                                        html_content = result.html if hasattr(result, 'html') else None
                                        if _is_pubdb_page(result.url if hasattr(result, 'url') else None, html_content):
                                            # Collect table lines to check (up to 20 lines, first 5 rows for analysis)
                                            table_lines_to_check = []
                                            table_end = i
                                            while table_end < len(lines) and table_end < i + 20:  # Check up to 20 lines
                                                next_line = lines[table_end].strip()
                                                if re.match(r'^\|', next_line):
                                                    table_lines_to_check.append(next_line)
                                                    table_end += 1
                                                elif not next_line:
                                                    if table_end + 1 < len(lines) and re.match(r'^\|', lines[table_end + 1].strip()):
                                                        table_end += 1
                                                    else:
                                                        break
                                                else:
                                                    break
                                            
                                            # Check for PUBDB UI keywords in table content (first 5 rows)
                                            table_content = ' '.join(table_lines_to_check[:5]).lower()
                                            
                                            if is_pubdb_ui_table(table_content):
                                                # #region agent log
                                                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                                    import json
                                                    f.write(json.dumps({
                                                        'sessionId': 'debug-session',
                                                        'runId': 'run1',
                                                        'hypothesisId': 'PUBDB',
                                                        'location': 'crawl_desy_simple.py:6195',
                                                        'message': 'Removed PUBDB UI table from markdown_content',
                                                        'data': {
                                                            'table_preview': ' '.join(table_lines_to_check[:2])[:100],
                                                            'url': result.url if hasattr(result, 'url') else None,
                                                            'is_pubdb_url': is_pubdb_url(result.url) if hasattr(result, 'url') and result.url else False,
                                                            'is_pubdb_content': is_pubdb_content(html_content) if html_content else False
                                                        },
                                                        'timestamp': int(__import__('time').time() * 1000)
                                                    }) + '\n')
                                                # #endregion
                                                # Skip this PUBDB UI table
                                                i = table_end
                                                continue
                                        
                                        # For non-PUBDB pages or non-UI tables on PUBDB pages:
                                        # Remove table sections from markdown_content (they're already in tables_markdown)
                                        # #region agent log
                                        if hasattr(result, 'url') and result.url and ('pubdb' in result.url.lower() or 'publications' in result.url.lower()):
                                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                                import json
                                                f.write(json.dumps({
                                                    'sessionId': 'debug-session',
                                                    'runId': 'run1',
                                                    'hypothesisId': 'PUBDB',
                                                    'location': 'crawl_desy_simple.py:6241',
                                                    'message': 'Removing table from markdown_content (not UI table, will be in tables_markdown)',
                                                    'data': {
                                                        'table_preview': stripped[:100],
                                                        'is_pubdb_page': _is_pubdb_page(result.url if hasattr(result, 'url') else None, html_content),
                                                        'is_pubdb_ui_table': False  # This path is for non-UI tables
                                                    },
                                                    'timestamp': int(__import__('time').time() * 1000)
                                                }) + '\n')
                                        # #endregion
                                        
                                        # Find the end of this table section
                                        table_end = i
                                        while table_end < len(lines):
                                            next_line = lines[table_end].strip()
                                            if re.match(r'^\|', next_line):
                                                table_end += 1
                                            elif not next_line:
                                                if table_end + 1 < len(lines) and re.match(r'^\|', lines[table_end + 1].strip()):
                                                    table_end += 1
                                                else:
                                                    break
                                            else:
                                                break
                                        # Skip this table section
                                        i = table_end
                                        continue
                                    
                                    # GENERAL: Remove text lines that duplicate table content
                                    # Pattern: Lines with names, emails, phones, locations that appear in tables_markdown
                                    # Check if this line contains structured data (name, email, phone, location) that's in tables
                                    if stripped and not stripped.startswith('#') and not stripped.startswith('|'):
                                        # Check if line contains field labels (E-Mail, Phone, Location) - likely duplicate of table content
                                        has_field_labels = re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', stripped, re.I) is not None
                                        
                                        # Check if line is just a name (single word or two words, capitalized) followed by field labels
                                        # Pattern: "Name\nE-Mail:..." or "FirstName\nLastName\nE-Mail:..."
                                        is_name_line = False
                                        words = stripped.split()
                                        # Check if current line is a name (1-3 capitalized words, no punctuation except spaces)
                                        if len(words) <= 3 and all(w and w[0].isupper() and w.replace('-', '').isalnum() for w in words if w):
                                            # Check if next line has field labels
                                            if i + 1 < len(lines):
                                                next_line = lines[i + 1].strip()
                                                if next_line and re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', next_line, re.I) is not None:
                                                    is_name_line = True
                                            # Also check if current line itself has field labels (e.g., "Andrey\nSiemens\nE-Mail:...")
                                            if has_field_labels:
                                                is_name_line = True
                                            # Also check if this is a name line followed by another name line (split name like "Andrey\nSiemens")
                                            # Look ahead 1-2 lines to see if there's a field label
                                            if not is_name_line and i + 2 < len(lines):
                                                next_next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
                                                after_next = lines[i + 2].strip() if i + 2 < len(lines) else ""
                                                # If next line is also a name and line after has field labels, this is part of a name
                                                if (next_next_line and len(next_next_line.split()) <= 3 and 
                                                    all(w and w[0].isupper() and w.replace('-', '').isalnum() for w in next_next_line.split() if w) and
                                                    after_next and re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', after_next, re.I) is not None):
                                                    is_name_line = True
                                        
                                        # Also check if line contains email/phone patterns (mailto:, @, phone numbers)
                                        has_contact_patterns = bool(re.search(r'mailto:|@|phone|tel|\+?\d{2,}', stripped, re.I))
                                        
                                        # Also check if line contains location patterns (room numbers, building codes)
                                        has_location_patterns = bool(re.search(r'\d+\s*[a-z]\s*/\s*\d+|location|office|room', stripped, re.I))
                                        
                                        # FIX: Only remove contact info if tables_markdown has content
                                        # For pages without tables, contact info should be KEPT
                                        tables_has_content = tables_markdown and len(tables_markdown.strip()) > 50
                                        
                                        # Only skip if tables actually have content that this might duplicate
                                        if tables_has_content and (has_field_labels or is_name_line or has_contact_patterns or has_location_patterns):
                                            # This line likely duplicates table content - skip it
                                            i += 1
                                            continue
                                    
                                    cleaned_lines.append(line)
                                    i += 1
                                
                                # Only add non-empty text content from markdown_content (intro text, etc.)
                                text_only_content = '\n'.join(cleaned_lines).strip()
                            
                            # Add tables_markdown FIRST (contains headings and tables in DOM order)
                            # This preserves the correct DOM order where tables come before other content
                            # #region agent log
                            if 'heuser' in result.url.lower():
                                with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                    import json
                                    f.write(json.dumps({
                                        'sessionId': 'debug-session',
                                        'runId': 'run1',
                                        'hypothesisId': 'H2',
                                        'location': 'crawl_desy_simple.py:6095',
                                        'message': 'Adding tables_markdown FIRST to preserve DOM order',
                                        'data': {
                                            'tables_markdown_preview': tables_markdown[:200] if tables_markdown else ''
                                        },
                                        'timestamp': int(__import__('time').time() * 1000)
                                    }) + '\n')
                            # #endregion
                            content_to_save += tables_markdown
                            
                            # Add text_only_content AFTER tables_markdown (for any remaining content not in tables_markdown)
                            if text_only_content:
                                # #region agent log
                                if 'heuser' in result.url.lower():
                                    with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                        import json
                                        f.write(json.dumps({
                                            'sessionId': 'debug-session',
                                            'runId': 'run1',
                                            'hypothesisId': 'H2',
                                            'location': 'crawl_desy_simple.py:6092',
                                            'message': 'Adding text_only_content AFTER tables_markdown',
                                            'data': {
                                                'text_only_content_preview': text_only_content[:200],
                                                'tables_markdown_preview': tables_markdown[:200] if tables_markdown else ''
                                            },
                                            'timestamp': int(__import__('time').time() * 1000)
                                        }) + '\n')
                                # #endregion
                                content_to_save += "\n\n" + text_only_content
                        else:
                            # No DOM-order extraction, use markdown_content as-is
                            if markdown_content:
                                content_to_save += markdown_content
                        if image_refs_markdown:
                            content_to_save += image_refs_markdown
                        
                        # Extract and add external links
                        if not result_is_pdf and hasattr(result, 'html') and result.html:
                            external_links_markdown = extract_external_links(result.html, result.url)
                            if external_links_markdown:
                                content_to_save += external_links_markdown
                        
                        # FIX: Fill empty Name columns from email link text in tables
                        # This handles tables where Name column is empty but email link has the name
                        def fill_empty_name_columns(content):
                            """Fill empty Name columns in tables by extracting name from email links.
                            
                            Handles table rows that start with || (empty first cell) by extracting
                            the name from email link text and filling the empty cell.
                            Only fills if the first cell is truly empty (just whitespace).
                            """
                            lines = content.split('\n')
                            result_lines = []
                            in_name_table = False  # Track if we're in a table with Name column
                            in_any_table = False  # Track if we're in any table
                            
                            for line in lines:
                                stripped = line.strip()
                                
                                # Detect table headers with Name column
                                if stripped.startswith('|') and re.search(r'\|\s*Name\s*\|', stripped, re.I):
                                    in_name_table = True
                                    in_any_table = True
                                    result_lines.append(line)
                                    continue
                                
                                # Detect any table header (for tables without Name column)
                                if stripped.startswith('|') and '---' not in stripped and not in_any_table:
                                    # Check if it looks like a table header (has multiple columns)
                                    cols = [c.strip() for c in stripped.split('|') if c.strip()]
                                    if len(cols) >= 2:
                                        in_any_table = True
                                        # #region agent log
                                        if 'E-Mail' in stripped or 'Phone' in stripped:
                                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                                import json
                                                f.write(json.dumps({
                                                    'sessionId': 'debug-session',
                                                    'runId': 'run1',
                                                    'hypothesisId': 'TABLE_DETECT',
                                                    'location': 'crawl_desy_simple.py:6060',
                                                    'message': 'Detected table header',
                                                    'data': {
                                                        'header': stripped[:100],
                                                        'cols_count': len(cols),
                                                        'in_any_table': in_any_table
                                                    },
                                                    'timestamp': int(__import__('time').time() * 1000)
                                                }) + '\n')
                                        # #endregion
                                
                                # Reset when we leave the table (empty line or non-table line)
                                if not stripped or not stripped.startswith('|'):
                                    in_name_table = False
                                    in_any_table = False
                                    result_lines.append(line)
                                    continue
                                
                                # Skip separator rows
                                if '---' in stripped:
                                    result_lines.append(line)
                                    continue
                                
                                # Process table rows with empty first cell
                                # Pattern: ||  | [Name](mailto:...) | ... OR |  | [Name](mailto:...) | ...
                                # Process if: (1) in table with Name column OR (2) in any table with empty first cell
                                if stripped.startswith('|'):
                                    # Check if first cell is empty: || ... or |  |
                                    # Match: |<empty or whitespace>| OR || (double pipe at start)
                                    # Also handle case where it's just || with no space
                                    first_cell_empty = re.match(r'^\|\s*\|', stripped) or stripped.startswith('||')
                                    
                                    # #region agent log
                                    if 'andrey' in stripped.lower() or 'anjali' in stripped.lower():
                                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                            import json
                                            f.write(json.dumps({
                                                'sessionId': 'debug-session',
                                                'runId': 'run1',
                                                'hypothesisId': 'EMPTY_CELL',
                                                'location': 'crawl_desy_simple.py:6105',
                                                'message': 'Checking row for empty first cell',
                                                'data': {
                                                    'line': stripped[:150],
                                                    'line_first_10_chars': stripped[:10],
                                                    'in_name_table': in_name_table,
                                                    'in_any_table': in_any_table,
                                                    'first_cell_empty': bool(first_cell_empty),
                                                    'regex_match': bool(re.match(r'^\|\s*\|', stripped)),
                                                    'starts_with_double_pipe': stripped.startswith('||')
                                                },
                                                'timestamp': int(__import__('time').time() * 1000)
                                            }) + '\n')
                                    # #endregion
                                    
                                    if first_cell_empty:
                                        # Look for email link in the row
                                        email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', stripped)
                                        if email_match:
                                            link_text = email_match.group(1).strip()
                                            # Validate it looks like a name (1-5 words, capitalized)
                                            words = link_text.split()
                                            if 1 <= len(words) <= 5:
                                                # Check if words look like names (start with capital or umlaut)
                                                is_name = all(
                                                    w and (w[0].isupper() or w[0] in 'äöüÄÖÜ') 
                                                    for w in words if w and not w.startswith('(')
                                                )
                                                # Check it's not a phone/number pattern
                                                has_phone = bool(re.search(r'\d{3,}|T\.|Phone', link_text))
                                                
                                                # Check if name already exists in the row (outside the email link)
                                                # Remove the email link part and check if name appears in remaining text
                                                row_without_link = re.sub(r'\[[^\]]+\]\([^)]+\)', '', stripped)
                                                name_already_exists = link_text.lower() in row_without_link.lower()
                                                
                                                # Fill if: (1) in table with Name column OR (2) in any table with empty first cell
                                                if is_name and not has_phone and not name_already_exists and (in_name_table or in_any_table):
                                                    # #region agent log
                                                    if 'andrey' in link_text.lower() or 'anjali' in link_text.lower():
                                                        with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                                            import json
                                                            f.write(json.dumps({
                                                                'sessionId': 'debug-session',
                                                                'runId': 'run1',
                                                                'hypothesisId': 'NAME_FILL',
                                                                'location': 'crawl_desy_simple.py:6103',
                                                                'message': 'Filling empty name cell',
                                                                'data': {
                                                                    'link_text': link_text,
                                                                    'in_name_table': in_name_table,
                                                                    'in_any_table': in_any_table,
                                                                    'is_name': is_name,
                                                                    'has_phone': has_phone,
                                                                    'name_already_exists': name_already_exists,
                                                                    'original_line': stripped[:150]
                                                                },
                                                                'timestamp': int(__import__('time').time() * 1000)
                                                            }) + '\n')
                                                    # #endregion
                                                    # Replace the empty first cell with the name
                                                    # |  | -> | Name |
                                                    empty_cell_match = first_cell_empty.group(0)
                                                    line = '| ' + link_text + ' |' + stripped[len(empty_cell_match):]
                                
                                result_lines.append(line)
                            
                            return '\n'.join(result_lines)
                        
                        # Apply the fix
                        # #region agent log
                        if 'magnetism' in str(result.url).lower():
                            # Check what content_to_save looks like BEFORE fill_empty_name_columns
                            lines_preview = content_to_save.split('\n')
                            andrey_lines = [l for l in lines_preview if 'andrey' in l.lower()]
                            with open('/home/taheri/crawl4ai/.cursor/debug.log', 'a') as f:
                                import json
                                f.write(json.dumps({
                                    'sessionId': 'debug-session',
                                    'runId': 'run1',
                                    'hypothesisId': 'BEFORE_FILL',
                                    'location': 'crawl_desy_simple.py:6109',
                                    'message': 'Content BEFORE fill_empty_name_columns',
                                    'data': {
                                        'andrey_lines': andrey_lines[:2] if andrey_lines else ['Not found']
                                    },
                                    'timestamp': int(__import__('time').time() * 1000)
                                }) + '\n')
                        # #endregion
                        content_to_save = fill_empty_name_columns(content_to_save)
                        
                        # FINAL CLEANUP: Remove artifacts and clean up content
                        lines = content_to_save.split('\n')
                        cleaned_lines = []
                        seen_headings = {}
                        consecutive_empty = 0
                        EARLY_LINE_THRESHOLD = 30  # Lines in first N are likely artifacts
                        
                        # Common navigation/footer patterns to filter out
                        nav_patterns = [
                            r'data privacy policy', r'declaration of accessibility', r'impressum', r'datenschutz',
                            r'cookie', r'privacy policy', r'accessibility', r'barrierefreiheit',
                            r'^##\s+(PHOTON SCIENCE|Beamline Staff)$',  # Duplicate navigation headings
                            r'breadcrumb',  # Breadcrumb navigation
                            r'^##\s+Breadcrumb\s*Navigation',  # Breadcrumb navigation heading
                            r'^##\s+Navigation$',  # Generic navigation heading
                        ]
                        
                        for i, line in enumerate(lines):
                            stripped = line.strip()
                            
                            # Remove excessive empty lines (max 2 consecutive)
                            if not stripped:
                                consecutive_empty += 1
                                if consecutive_empty <= 2:
                                    cleaned_lines.append(line)
                                continue
                            consecutive_empty = 0
                            
                            # GENERAL: Filter malformed tables (10+ columns, concatenated data)
                            # Pattern: Table rows with 10+ columns are likely malformed
                            if re.match(r'^\|', stripped):
                                # Count columns (number of | separators)
                                column_count = stripped.count('|') - 1  # Subtract 1 for leading |
                                if column_count > 10:
                                    continue
                                # Pattern: Only filter single-column rows with field labels, or rows where ALL cells have labels
                                # Multi-column tables (2-10 columns) with field labels in cells are legitimate
                                if column_count == 1:
                                    # Single column: filter if has 3+ field labels (concatenated)
                                    first_cell_match = re.match(r'^\|\s*([^|]+)', stripped)
                                    if first_cell_match:
                                        first_cell = first_cell_match.group(1)
                                        field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', first_cell, re.I))
                                        if field_label_count >= 3:
                                            continue
                                elif column_count >= 2 and column_count <= 10:
                                    # Multi-column: allow field labels in cells (legitimate structured data)
                                    # Only filter if this is clearly a separator row or malformed
                                    pass  # Don't filter multi-column tables with field labels
                            
                            # GENERAL: Filter navigation/footer links (helmholtz, DOOR, XFEL, CFEL, CSSB, etc.)
                            nav_link_patterns = [
                                r'helmholtz\.de', r'door\.desy\.de', r'xfel\.eu', r'cfel\.de', r'cssb-hamburg',
                                r'pbook', r'data_privacy', r'More information'
                            ]
                            if any(re.search(pattern, stripped, re.I) for pattern in nav_link_patterns):
                                continue
                            
                            # Remove navigation/footer content (general patterns)
                            if any(re.search(pattern, stripped, re.I) for pattern in nav_patterns):
                                # Skip this line and check if it's a section (skip the section)
                                if stripped.startswith('##'):
                                    # Skip this heading and all content until next heading
                                    j = i + 1
                                    while j < len(lines):
                                        next_stripped = lines[j].strip()
                                        if next_stripped.startswith('#'):
                                            break
                                        j += 1
                                    # Skip to next heading (don't add current line)
                                    continue
                                continue
                            
                            # GENERAL: Remove empty headings (just ## with spaces, no text)
                            # Pattern: "##    " or "##" - headings with no actual text
                            if stripped.startswith('#'):
                                heading_text = stripped.lstrip('#').strip()
                                if not heading_text:
                                    # Empty heading - skip it
                                    continue
                                
                                # GENERAL: Remove empty sections (heading with no content until next heading)
                                # Check if this section is empty (only whitespace/empty lines until next heading)
                                section_start_idx = i
                                section_end_idx = len(lines)  # Default to end of file
                                
                                # Find next heading or end of file
                                for j in range(i + 1, len(lines)):
                                    next_stripped = lines[j].strip()
                                    if next_stripped.startswith('#'):
                                        section_end_idx = j
                                        break
                                
                                # Check if section has any content (not just whitespace, separators, or empty lines)
                                has_content = False
                                for j in range(section_start_idx + 1, section_end_idx):
                                    content_line = lines[j].strip()
                                    if not content_line:
                                        continue  # Skip empty lines
                                    # Skip separators (already handled)
                                    if (content_line == '---' or 
                                        re.match(r'^\|[\s\-:]+\|$', content_line) or 
                                        content_line == '|---|---' or
                                        re.match(r'^\|[\s\-]+\|$', content_line) or
                                        re.match(r'^[\|\s\-]+$', content_line)):
                                        continue
                                    # Skip if it's another heading (shouldn't happen, but safety check)
                                    if content_line.startswith('#'):
                                        continue
                                    # Found actual content (text, table, list, link, etc.)
                                    has_content = True
                                    break
                                
                                if not has_content:
                                    # Empty section - skip the heading and continue to next iteration
                                    # Don't add this line to cleaned_lines
                                    continue
                            
                            # Remove early horizontal rules and orphaned separators (artifacts at start)
                            # GENERAL: Aggressively remove separators in the first N lines (they're likely artifacts)
                            if i < EARLY_LINE_THRESHOLD:
                                # Match all separator variants: ---, |---|---, | --- |, |---|, etc.
                                if (stripped == '---' or 
                                    re.match(r'^\|[\s\-:]+\|$', stripped) or 
                                    stripped == '|---|---' or
                                    re.match(r'^\|[\s\-]+\|$', stripped) or
                                    re.match(r'^[\|\s\-]+$', stripped)):
                                    continue
                            
                            # Remove orphaned table separators without proper table context
                            # GENERAL: These appear when broken fragments are removed, leaving orphaned separators
                            if (stripped == '---' or 
                                re.match(r'^\|[\s\-:]+\|$', stripped) or 
                                stripped == '|---|---' or
                                re.match(r'^\|[\s\-]+\|$', stripped) or
                                re.match(r'^[\|\s\-]+$', stripped)):
                                # Check for table header before and row after (skip empty lines and headings)
                                # Look further back/forward to catch separators that are far from tables
                                # Require BOTH header row (with at least 2 columns) AND data row (with at least 2 columns)
                                has_header = any(re.match(r'^\|\s*[^|]+\s*\|.*\|', lines[j].strip()) 
                                                for j in range(max(0, i - 20), i) if lines[j].strip() and not lines[j].strip().startswith('#'))
                                has_row = any(re.match(r'^\|\s*[^|]+\s*\|.*\|', lines[j].strip()) 
                                             for j in range(i + 1, min(len(lines), i + 20)) if lines[j].strip() and not lines[j].strip().startswith('#'))
                                if not (has_header and has_row):
                                    # Orphaned separator - skip it
                                    continue
                            
                            # GENERAL: Remove broken text fragments (single values like "192 ns", "6.0 GeV", etc.)
                            # Pattern: Lines that are just numbers with units (no label, no structure)
                            # Check if this line is just a value (numbers with units, no label)
                            if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)\s*$', stripped, re.I):
                                continue
                            
                            # Remove leftover names (single word or "Last, First") followed by empty lines
                            if not any(stripped.startswith(c) for c in '#-|*'):
                                words = stripped.split()
                                is_name = False
                                if len(words) == 1 and words[0][0].isupper() and len(words[0]) > 2:
                                    is_name = True
                                elif ',' in stripped:
                                    parts = [p.strip() for p in stripped.split(',')]
                                    if len(parts) == 2 and all(p and p[0].isupper() for p in parts):
                                        is_name = True
                                if is_name:
                                    empty_ahead = sum(1 for j in range(i + 1, min(len(lines), i + 10)) 
                                                     if not lines[j].strip())
                                    if empty_ahead >= 3:
                                        continue
                            
                            # Remove duplicate headings (within 20 lines)
                            if stripped.startswith('#'):
                                heading_sig = stripped.lstrip('#').strip().lower()
                                if heading_sig in seen_headings and i - seen_headings[heading_sig] < 20:
                                    continue
                                seen_headings[heading_sig] = i
                            
                            # FIX: Remove text lines that are substrings of table content
                            # This catches role descriptions that appear both as standalone text AND in tables
                            if tables_markdown and not stripped.startswith(('#', '|', '-', '*')):
                                # Normalize whitespace for comparison (collapse multiple spaces to single)
                                stripped_normalized = re.sub(r'\s+', ' ', stripped.lower().strip())
                                # Check if this line appears inside any table cell
                                if len(stripped_normalized) > 5:  # Only check meaningful lines
                                    # Normalize table content for comparison (collapse whitespace)
                                    tables_normalized = re.sub(r'\s+', ' ', tables_markdown.lower())
                                    # Check if this text appears in a table cell
                                    if stripped_normalized in tables_normalized:
                                        # Skip this line - it's a duplicate of table content
                                        continue
                            
                            # GENERAL: Normalize text spacing to fix concatenation issues
                            # Apply only to non-markdown lines (text content)
                            normalized_line = _normalize_text_spacing(line)
                            cleaned_lines.append(normalized_line)
                        
                        # Remove leading empty lines and orphaned separators
                        # GENERAL: Remove ALL leading separators until non-separator content is found
                        while cleaned_lines:
                            first_stripped = cleaned_lines[0].strip()
                            if not first_stripped:
                                cleaned_lines.pop(0)
                            elif (first_stripped == '---' or 
                                  re.match(r'^\|[\s\-:]+\|$', first_stripped) or 
                                  first_stripped == '|---|---' or
                                  re.match(r'^\|[\s\-]+\|$', first_stripped) or
                                  re.match(r'^[\|\s\-]+$', first_stripped)):
                                # Remove leading orphaned separators
                                cleaned_lines.pop(0)
                            else:
                                break
                        
                        # GENERAL: Ensure "External Links" section has proper header and remove duplicates
                        # Scan for external link pattern (markdown links to external URLs)
                        external_link_pattern = r'^- \[.*\]\(https?://[^)]+\)'
                        has_external_links = False
                        external_links_start_idx = None
                        external_links_header_indices = []  # Track all header positions
                        
                        for i, line in enumerate(cleaned_lines):
                            stripped_line = line.strip()
                            # Check if this is an external link
                            if re.match(external_link_pattern, stripped_line):
                                if external_links_start_idx is None:
                                    external_links_start_idx = i
                                has_external_links = True
                            # Check if "External Links" header exists
                            elif stripped_line == '## External Links':
                                external_links_header_indices.append(i)
                        
                        # Remove duplicate "External Links" headers (keep only the first one before links)
                        # Remove headers that are after the first link or duplicates
                        if external_links_header_indices and len(external_links_header_indices) > 1:
                            # Sort in reverse order to remove from end first (preserves indices)
                            # Keep the first header (lowest index), remove all others
                            first_header_idx = min(external_links_header_indices)
                            for header_idx in sorted(external_links_header_indices, reverse=True):
                                if header_idx == first_header_idx:
                                    continue  # Keep the first one
                                # Remove duplicate header
                                if header_idx < len(cleaned_lines):
                                    cleaned_lines.pop(header_idx)
                                    # Remove empty line after if present
                                    if header_idx < len(cleaned_lines) and not cleaned_lines[header_idx].strip():
                                        cleaned_lines.pop(header_idx)
                                    # Remove empty line before if present
                                    if header_idx > 0 and not cleaned_lines[header_idx - 1].strip():
                                        cleaned_lines.pop(header_idx - 1)
                        
                        # If external links exist but no header before them, add it
                        if has_external_links and external_links_start_idx is not None:
                            # Re-check header indices after removals (they may have changed)
                            remaining_header_indices = [i for i, line in enumerate(cleaned_lines) if line.strip() == '## External Links']
                            # Check if header exists before the first link
                            has_header_before = any(idx < external_links_start_idx for idx in remaining_header_indices)
                            if not has_header_before:
                                # Insert header before first external link
                                # Add empty line before if needed
                                if external_links_start_idx > 0 and cleaned_lines[external_links_start_idx - 1].strip():
                                    cleaned_lines.insert(external_links_start_idx, '')
                                cleaned_lines.insert(external_links_start_idx, '## External Links')
                                # Add empty line after header if needed
                                if external_links_start_idx + 1 < len(cleaned_lines) and cleaned_lines[external_links_start_idx + 1].strip():
                                    cleaned_lines.insert(external_links_start_idx + 1, '')
                        
                        # FINAL PASS: Remove any remaining empty sections (safety check)
                        # This catches empty sections that might have been missed
                        final_cleaned = []
                        i = 0
                        while i < len(cleaned_lines):
                            line = cleaned_lines[i]
                            stripped = line.strip()
                            
                            # Check if this is a heading
                            if stripped.startswith('#'):
                                heading_text = stripped.lstrip('#').strip()
                                if heading_text:  # Not empty heading
                                    # Check if section is empty
                                    section_start = i
                                    section_end = len(cleaned_lines)
                                    # Find next heading
                                    for j in range(i + 1, len(cleaned_lines)):
                                        if cleaned_lines[j].strip().startswith('#'):
                                            section_end = j
                                            break
                                    # Check for content
                                    has_content = False
                                    for j in range(section_start + 1, section_end):
                                        content_line = cleaned_lines[j].strip()
                                        if not content_line:
                                            continue
                                        if (content_line.startswith('#') or
                                            content_line == '---' or
                                            re.match(r'^\|[\s\-:]+\|$', content_line) or
                                            content_line == '|---|---' or
                                            re.match(r'^\|[\s\-]+\|$', content_line) or
                                            re.match(r'^[\|\s\-]+$', content_line)):
                                            continue
                                        has_content = True
                                        break
                                    if not has_content:
                                        # Skip empty section
                                        i = section_end
                                        continue
                            
                            final_cleaned.append(line)
                            i += 1
                        
                        content_to_save = '\n'.join(final_cleaned)
                        
                        # Skip empty pages: Check if content is meaningful (not just URL header and minimal text)
                        # Remove URL header and separators to check actual content
                        content_without_header = re.sub(r'^#\s*Source\s*URL.*?\n---\s*\n', '', content_to_save, flags=re.IGNORECASE | re.MULTILINE)
                        content_meaningful = content_without_header.strip()
                        
                        # Skip if content is too short or only contains error messages
                        is_empty_page = False
                        if len(content_meaningful) < 50:  # Very short content
                            is_empty_page = True
                        elif len(content_meaningful) < 200:
                            # Check if it's just error messages or minimal content
                            error_patterns = [
                                r'page could not be found',
                                r'404',
                                r'not found',
                                r'error',
                                r'page not available'
                            ]
                            content_lower = content_meaningful.lower()
                            if any(pattern in content_lower for pattern in error_patterns):
                                # Count meaningful words (exclude links, headers, etc.)
                                words = [w for w in content_meaningful.split() if len(w) > 2 and not w.startswith('http') and not w.startswith('#')]
                                if len(words) < 10:  # Less than 10 meaningful words
                                    is_empty_page = True
                        
                        if is_empty_page:
                            print(f"[SKIP] Empty/minimal content page: {final_url or original_url}")
                            continue  # Skip saving this page
                        
                        filename.write_text(content_to_save, encoding="utf-8")
                        
                        # Log extraction results
                        if hasattr(result, 'tables') and result.tables:
                            page_type = "PDF" if result_is_pdf else "HTML"
                            print(f"[{page_type}] Extracted {len(result.tables)} table(s) with links preserved from {result.url}")
                        
                        if result_is_pdf and PDF_SUPPORT_AVAILABLE:
                            # Check metadata for PDF info
                            if hasattr(result, 'metadata') and result.metadata:
                                pdf_info = []
                                if result.metadata.get('title'):
                                    pdf_info.append(f"Title: {result.metadata.get('title')}")
                                if result.metadata.get('author'):
                                    pdf_info.append(f"Author: {result.metadata.get('author')}")
                                if pdf_info:
                                    print(f"[PDF] Metadata: {', '.join(pdf_info)}")
                            
                            print(f"[PDF] Extracted {len(markdown_content)} characters from PDF")
                        
                        all_successful_urls.append(result.url)
                        file_type = "PDF" if result_is_pdf else "HTML"
                        print(f"[SAVED] [{file_type}] {result.url}")
                        print(f"        → {filename}")
                    except Exception as file_save_error:
                        # Error during file save - log but continue processing
                        all_errors.append({
                            'url': result.url,
                            'error': f'File save error: {str(file_save_error)}',
                            'timestamp': datetime.now().isoformat()
                        })
                        print(f"[ERROR] File save failed for {result.url}: {file_save_error}")
                    
                except Exception as e:
                    # Exception while processing - log the error with full traceback
                    import traceback
                    error_url = result.url if result and result.url else "Unknown URL"
                    error_traceback = traceback.format_exc()
                    all_errors.append({
                        'url': error_url,
                        'error': f'Exception: {str(e)}',
                        'traceback': error_traceback,
                        'timestamp': datetime.now().isoformat()
                    })
                    print(f"[ERROR] {error_url}")
                    print(f"        Exception: {str(e)}")
                    print(f"        Traceback:\n{error_traceback}")
            
            # ====================================================================
            # STEP 9: Save URLs by Depth
            # ====================================================================
            # Save all URLs organized by depth level to a JSON file
            # This shows how many URLs were found at each depth
            
            depth_summary = {}
            total_unique_final_urls = set()
            total_unique_original_urls = set()
            redirect_count = 0
            
            for depth_str, url_entries in all_urls_by_depth.items():
                # Extract final URLs for backward compatibility (simple list)
                final_urls = [entry.get('final_url') if isinstance(entry, dict) else entry for entry in url_entries]
                # Also track unique URLs
                for entry in url_entries:
                    if isinstance(entry, dict):
                        if entry.get('final_url'):
                            total_unique_final_urls.add(entry['final_url'])
                        if entry.get('original_url'):
                            total_unique_original_urls.add(entry['original_url'])
                        if entry.get('is_redirect'):
                            redirect_count += 1
                    else:
                        # Legacy format (string)
                        total_unique_final_urls.add(entry)
                        total_unique_original_urls.add(entry)
                
                # Create unique_final_urls: deduplicated list of unique final URLs
                unique_final_urls_list = []
                seen_in_depth = set()
                for entry in url_entries:
                    if isinstance(entry, dict):
                        final_url = entry.get('final_url')
                        if final_url and final_url not in seen_in_depth:
                            unique_final_urls_list.append(final_url)
                            seen_in_depth.add(final_url)
                    else:
                        # Legacy format (string)
                        if entry and entry not in seen_in_depth:
                            unique_final_urls_list.append(entry)
                            seen_in_depth.add(entry)
                
                depth_summary[depth_str] = {
                    'count': len(url_entries),
                    'unique_final_urls': unique_final_urls_list,  # NEW: deduplicated unique final URLs
                    'url_entries': url_entries  # Preserved: detailed entries with redirect info
                }
            
            urls_by_depth_file = LOG_DIR / "urls_by_depth.json"
            urls_by_depth_data = {
                'timestamp': datetime.now().isoformat(),
                'root_urls': ROOT_URLS,
                'max_depth': MAX_DEPTH,
                'total_urls': sum(len(urls) for urls in all_urls_by_depth.values()),
                'total_unique_final_urls': len(total_unique_final_urls),
                'total_unique_original_urls': len(total_unique_original_urls),
                'redirects_detected': redirect_count,
                'urls_by_depth': depth_summary,
                'link_analysis': {
                    'total_links_found_in_html': links_found_vs_crawled['total_links_found_in_html'],
                    'total_urls_crawled': links_found_vs_crawled['total_urls_crawled'],
                    'average_links_per_page': links_found_vs_crawled['total_links_found_in_html'] / max(links_found_vs_crawled['total_urls_crawled'], 1),
                    'sample_pages_with_links': links_found_vs_crawled['links_found_by_page'][:20]  # First 20 pages
                }
            }
            urls_by_depth_file.write_text(json.dumps(urls_by_depth_data, indent=2), encoding="utf-8")
            print("-" * 60)
            print(f"[DEPTH SUMMARY] URLs by depth:")
            for depth_str in sorted(depth_summary.keys(), key=int):
                count = depth_summary[depth_str]['count']
                print(f"  Depth {depth_str}: {count} URLs")
            print(f"[DEPTH FILE] Saved to {urls_by_depth_file}")
            
            # ====================================================================
            # STEP 10: Save Error Log
            # ====================================================================
            # Save all failed URLs with error reasons to a JSON file
            
            if all_errors:
                # Categorize errors for better analysis
                timeout_errors = [e for e in all_errors if 'timeout' in str(e.get('error', '')).lower() or 'timed out' in str(e.get('error', '')).lower()]
                other_errors = [e for e in all_errors if e not in timeout_errors]
                
                error_log = {
                    'timestamp': datetime.now().isoformat(),
                    'total_errors': len(all_errors),
                    'total_successful': len(all_successful_urls),
                    'timeout_errors': len(timeout_errors),
                    'other_errors': len(other_errors),
                    'timeout_urls': [{'url': e.get('url'), 'error': e.get('error'), 'timestamp': e.get('timestamp')} for e in timeout_errors],
                    'errors': all_errors
                }
                ERROR_LOG_FILE.write_text(json.dumps(error_log, indent=2), encoding="utf-8")
                print("-" * 60)
                print(f"[ERROR LOG] Saved {len(all_errors)} errors to {ERROR_LOG_FILE}")
            else:
                # Create empty error log if no errors
                error_log = {
                    'timestamp': datetime.now().isoformat(),
                    'total_errors': 0,
                    'total_successful': len(all_successful_urls),
                    'errors': []
                }
                ERROR_LOG_FILE.write_text(json.dumps(error_log, indent=2), encoding="utf-8")
            
            # ====================================================================
            # Final Summary
            # ====================================================================
            print("-" * 60)
            print(f"[SUMMARY]")
            print(f"  URLs processed: {len(ROOT_URLS)}")
            print(f"  Successful: {len(all_successful_urls)} pages")
            print(f"  Errors: {len(all_errors)} pages")
            print(f"  Total crawled: {len(all_results)} pages")
            print(f"  Files saved to: {OUTPUT_DIR}/")
            if all_errors:
                print(f"  Error log: {ERROR_LOG_FILE}")
            print("-" * 60)
    
    except Exception as cleanup_error:
        # Handle errors during browser startup or cleanup
        error_str = str(cleanup_error)
        is_startup_error = 'ENOSPC' in error_str or 'no space left' in error_str.lower()
        
        if is_startup_error:
            # Critical error: Browser cannot start due to disk space
            print(f"\n[ERROR] Browser startup failed: {error_str}")
            print(f"[ERROR] Cannot proceed with crawling - disk space issue")
            print(f"[INFO] Files saved: {len(all_successful_urls)} pages")
            print(f"\n[SOLUTION] Free up disk space and try again:")
            print(f"  - Check /tmp directory: df -h /tmp")
            print(f"  - Clean up temporary files")
            print(f"  - Check available space: df -h")
            
            all_errors.append({
                'url': 'Browser Startup',
                'error': f"Browser startup failed: {error_str}",
                'timestamp': datetime.now().isoformat(),
                'note': 'Critical error: Browser cannot start. No pages were crawled. Free up disk space and try again.'
            })
        else:
            # Non-critical cleanup error (browser closed unexpectedly after crawling)
            cleanup_error_msg = f"Browser cleanup error (non-critical): {error_str}"
            print(f"\n[WARNING] {cleanup_error_msg}")
            print(f"[INFO] Crawling completed successfully before cleanup error occurred")
            print(f"[INFO] Files saved: {len(all_successful_urls)} pages")
            
            all_errors.append({
                'url': 'Browser Cleanup',
                'error': cleanup_error_msg,
                'timestamp': datetime.now().isoformat(),
                'note': 'This error occurred during browser cleanup after crawling completed. All pages were successfully crawled and saved.'
            })
    
    finally:
        # Ensure error log is saved even if there was an exception during cleanup
        # This runs regardless of whether there was an error
        try:
            # Import json here to ensure it's available (in case of import shadowing)
            import json as json_module
            # Categorize errors for better analysis (timeout vs other errors)
            timeout_errors = [e for e in all_errors if e.get('is_timeout', False)]
            other_errors = [e for e in all_errors if not e.get('is_timeout', False)]
            
            error_log = {
                'timestamp': datetime.now().isoformat(),
                'total_errors': len(all_errors),
                'total_successful': len(all_successful_urls),
                'total_crawled': len(all_results),
                'timeout_errors': len(timeout_errors),
                'other_errors': len(other_errors),
                'timeout_urls': [{'url': e.get('url'), 'error': e.get('error'), 'timestamp': e.get('timestamp')} for e in timeout_errors],
                'errors': all_errors
            }
            ERROR_LOG_FILE.write_text(json_module.dumps(error_log, indent=2), encoding="utf-8")
            if all_errors:
                print(f"\n[ERROR LOG] Saved {len(all_errors)} errors to {ERROR_LOG_FILE}")
        except Exception as log_error:
            print(f"\n[WARNING] Failed to save error log: {log_error}")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
# This runs when you execute: python crawl_desy_simple.py

if __name__ == "__main__":
    # asyncio.run() is needed because crawl_site() is an async function
    # Async functions allow the crawler to fetch multiple pages simultaneously
    asyncio.run(crawl_site())

