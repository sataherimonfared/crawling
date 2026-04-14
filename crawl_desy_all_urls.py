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
import time
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from html import unescape
from urllib.parse import urlparse, urljoin

# aiohttp for async redirect resolution (non-blocking HEAD requests)
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    print("[WARNING] aiohttp not available - redirect resolution will use synchronous fallback")

# URL utility functions (extracted in refactoring Step 2)
import url_utils as _url_utils

# Table processing functions (extracted in refactoring Step 3)
import table_processing as _table_processing

# Content extraction functions (extracted in refactoring Step 4)
import content_extraction as _content_extraction
import markdown_cleanup as _markdown_cleanup
import checkpoint as _checkpoint


# Crawl4AI is a required runtime dependency for this script.
# Keep the failure mode explicit and actionable (no URL-specific behavior).
try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
    from crawl4ai import MemoryAdaptiveDispatcher, RateLimiter
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

def _get_cached_soup(result):
    """Delegate to content_extraction."""
    return _content_extraction._get_cached_soup(result)


def _clear_soup_cache():
    """Delegate to content_extraction."""
    _content_extraction._clear_soup_cache()

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

# FEATURE #4: Content Deduplication using ContentHash
# Detect and skip pages with duplicate content (same navbar/sidebar)
# This solves the Sara Taheri issue where identical content is accumulated
try:
    from crawl4ai.content_hash import ContentHash
    CONTENT_HASH_AVAILABLE = True
except ImportError:
    try:
        # Alternative import path for different Crawl4AI versions
        from crawl4ai.content_comparison import ContentHash
        CONTENT_HASH_AVAILABLE = True
    except ImportError:
        CONTENT_HASH_AVAILABLE = False
        print("[INFO] ContentHash not available - deduplication will use standard method")

# ============================================================================
# CONFIGURATION - Adjust these values to change crawling behavior
# ============================================================================

# List of URLs to crawl
ROOT_URLS = [
    "https://it.desy.de/index_eng.html",
    "https://it.desy.de/index_ger.html",
    # # "https://www.desy.de",
    # "https://desy.de/index_ger.html",
    # "https://desy.de/index_eng.html",
    # "https://www.desy.de/aktuelles/veranstaltungen/index_ger.html",
    # "https://www.desy.de/ueber_desy/leitende_wissenschaftler/christian_schwanenberger/index_ger.html",
    # "https://www.desy.de/career/contact/index_eng.html",
    # "https://photon-science.desy.de/facilities/petra_iii/machine/parameters/index_eng.html",
    # # Events page (should extract events)

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
    
    # "https://ai.desy.de/people/heuser.html",
    # "https://belle2.desy.de/",
    # "https://indico.desy.de/event/52144/",
    # "https://indico.desy.de/event/51380/page/5682-satellite-meetings",
    # "https://indico.desy.de/event/51547/",
]

# Restrict crawling to URLs that start with these prefixes.
# With the current seeds, this keeps the crawl inside the DESY IT site only.
# To re-enable every DESY subdomain later, set this tuple to ("https://desy.de/",)
# and change _is_allowed_crawl_url() to return _is_desy_domain(urlparse(url).netloc).
ALLOWED_URL_PREFIXES = (
    "https://it.desy.de/",
)






# Directory where crawled pages will be saved as markdown files
OUTPUT_DIR = Path("desy_crawled/26")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist

# Log directory for all log files
LOG_DIR = OUTPUT_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

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

# Domains to exclude from crawling and scraping at all depth levels (never queue, never fetch)
# Links to these hosts are skipped; URLs that redirect to these hosts are also skipped (redirect resolved before queuing)
EXCLUDED_DOMAINS = {
    'fater.desy.de',
    'bib-pubdb1.desy.de',
    # webcast.desy.de is intentionally NOT excluded — video pages contain meaningful
    # descriptions/abstracts next to each entry that are valuable for RAG.
}
# Resolve redirects before queuing so we never fetch excluded hosts (e.g. desy.de/some/path -> fater.desy.de)
CHECK_REDIRECTS_TO_EXCLUDED = True

# Debug log file removed - no longer needed

# Checkpoint/Resume settings
# Set to True to resume from previous checkpoint (skip already processed URLs)
# Set to False to start fresh (ignore previous checkpoint)
#
# HOW CHECKPOINT + DEPTH INCREASE WORKS:
#   1. First run:  USE_CHECKPOINT=True, MAX_DEPTH=3  → full BFS crawl up to depth 3.
#   2. Resume run: USE_CHECKPOINT=True, MAX_DEPTH=4  → checkpoint detects that depth 3
#      was already crawled. It sets _resuming_deeper=True automatically, which:
#      - Skips BFS re-crawl of all depth 0-3 pages (saves hours of re-fetching)
#      - Fetches only "frontier" pages (depth=3) from cache for link extraction
#      - Crawls only genuinely NEW URLs at depth 4
#   3. Same depth:  USE_CHECKPOINT=True, MAX_DEPTH=3  → seed URL skipped entirely.
#   4. Fresh start: USE_CHECKPOINT=False              → ignores checkpoint, full BFS.
#
# TL;DR: Just increase MAX_DEPTH and re-run with USE_CHECKPOINT=True.
#        The crawler automatically skips already-crawled pages.
USE_CHECKPOINT = True # False # Change to True to resume from checkpoint

# Checkpoint frequency: save checkpoint every N pages
CHECKPOINT_FREQUENCY = 5000  # Save progress every 5000 pages (reduced I/O vs 1000)

# Perf #13: cache mode depends on run type.
# 'read_write' when resuming (reads cached pages to skip re-fetching).
# 'write_only' for fresh runs (writes cache for future resume but skips read overhead).
CRAWL_CACHE_MODE = 'read_write' if USE_CHECKPOINT else 'write_only'

# ============================================================================
# GROUP 1: LOGIN/AUTH/ADMIN URL FILTERING
# ============================================================================
# Skip URLs that point to login, authentication, or admin pages
# These pages are typically empty auth forms with no useful content
# Expected impact: ~160-200 URLs per depth × 8 depths = ~1000+ URLs saved per crawl

# Regex patterns to detect login/auth/admin URLs
LOGIN_AUTH_ADMIN_PATTERNS = [
    r'/login',
    r'/login_form',
    r'/auth',
    r'/authenticate',
    r'/authorization',
    r'/signin',
    r'/register',
    r'/enrollment',
    r'/acl_users',
    r'/admin',
    r'/manage',
    r'/edit_entry\.php',
    r'/selfregistration',
    r'/pls/apex',
    r'/accounts',
    r'\?destination=',
    r'\?came_from=',
    r'\?login',
    r'&manage',
    r'\?manage'
]

# Compile patterns once for performance
_COMPILED_LOGIN_AUTH_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in LOGIN_AUTH_ADMIN_PATTERNS]

# Domains that ONLY serve auth/login pages (no content)
AUTH_ONLY_DOMAINS = {
    'selfregistration.desy.de',
    'sso.desy.de',
    'idp.desy.de',
    'keycloak.desy.de'
}

# ============================================================================
# GROUP 2: ERROR PAGES FILTERING (503, 404, TIMEOUT)
# ============================================================================
# Skip URLs known to return errors (backend failures, not found, timeouts)
# These pages contain error messages with no useful content
# Expected impact: ~132 error pages in depth_2, more at deeper levels
# Note: Only applies to DESY domains, no external domain concerns

# Regex patterns to detect URLs that are likely to error
ERROR_URL_PATTERNS = [
    r'/error',
    r'/404',
    r'/403',
    r'/500',
    r'/503',
    r'/maintenance',
    r'/unavailable',
    r'/offline',
    r'/notfound',
    r'\.error',
    r'/sitemap',  # Some old DESY sitemaps cause issues
    r'/cgi-bin',  # Legacy CGI scripts often error
]

# Compile patterns once for performance
_COMPILED_ERROR_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in ERROR_URL_PATTERNS]

# Domains or domain patterns that frequently timeout or error (DESY-specific)
ERROR_PRONE_DOMAINS = {
    'bib-pubdb1.desy.de',  # Already excluded, but mark as error-prone
    'fater.desy.de',  # Already excluded, but mark as error-prone
    'old.desy.de',  # Archived/very old server
    'legacy.desy.de',  # Legacy systems
}

# ============================================================================
# GROUP 4: QUERY PARAMETER DEDUPLICATION
# ============================================================================
# Skip URLs that are duplicates of already-crawled URLs (differ only in UI-only query params)
# These pages produce IDENTICAL content but are treated as different URLs
# Expected impact: ~3-5x multiplication reduction from query parameter variations

# Query parameters that are UI-only (safe to remove for deduplication)
# These don't change page content, just presentation/session/tracking
UI_ONLY_QUERY_PARAMS = {
    'printversion',      # Print-optimized version — same content, different layout
    'embed',             # Embedded view — same content
    'notoolbar',         # Hide toolbar — same content
    'lang',              # Language toggle — usually same content
    'came_from',         # Auth redirect destination — not content
    'destination',       # Auth redirect destination — not content
    'utm_source',        # Analytics tracking — not content
    'utm_medium',        # Analytics tracking — not content
    'utm_campaign',      # Analytics tracking — not content
    'sid',               # Session ID — not content
    'token',             # Auth token — not content
    'nonce',             # CSRF token — not content
    'redirect',          # Post-login redirect — not content
    'return_to',         # Return destination — not content
    # FIX 6a: New UI-only params found in Run 16 depth_2 analysis
    'view',              # Calendar view variant (workWeek/month) — same events
    'two_columns',       # Layout toggle — same content
    'opendirectanchor',  # Anchor scroll position — same page content
    'selected',          # UI selection state — same content
    'editing',           # Edit mode toggle — same content
    'openfile',          # File panel state — same content
    'timetohighlight',   # Calendar highlight — same content
    'year',              # Calendar year — low-value pagination
    'month',             # Calendar month — low-value pagination
    'day',               # Calendar day — low-value pagination
    'date',              # Calendar date — low-value pagination
    'period',            # Calendar period — low-value pagination
    'area',              # Room/area filter — UI state
    'room',              # Room filter — UI state
    # Zimbra calendar event instance params — same event text, different rendering slot
    'inststarttime',    # Unix ms timestamp of this recurring instance
    'instduration',     # Duration of this instance
    'useinstance',      # 0/1 show single instance vs. full series
    'pstat',            # Participation status (NE=new, AC=accepted)
}

# Query parameters that ARE content-critical (keep them in deduplication)
# Different values = different content, should crawl both
CONTENT_CRITICAL_PARAMS = {
    'q',           # Search query - different search = different content
    'page',        # Pagination - different page = different content
    'num',         # Page number - different page = different content
    'start',       # Pagination offset - different results = different content
    'category',    # Category filter - different category = different content
    'sort',        # Sort order - different sort = different content (sometimes)
    'filter',      # Custom filter - different filter = different content
}

# ============================================================================
# PHASE 1 FIX: Checkpointing Functions for Crash Recovery
# ============================================================================
# These functions save and load crawler state to enable resuming after crashes

def save_checkpoint(checkpoint_data: dict) -> bool:
    """Delegate to checkpoint module."""
    return _checkpoint.save_checkpoint(checkpoint_data, CHECKPOINT_FILE)


def load_checkpoint() -> dict:
    """Delegate to checkpoint module."""
    return _checkpoint.load_checkpoint(CHECKPOINT_FILE, USE_CHECKPOINT)

# How many pages to crawl simultaneously (parallelism)
# Maximum depth to crawl (how many link levels to follow)
# 0 = only the root page
# 1 = root page + pages linked from root (you found 33 URLs here)
# 2 = root + depth 1 pages + pages linked from depth 1 pages (you found 852 URLs here)
MAX_DEPTH = 8

# Higher = faster but uses more resources; scaled down for deeper crawls to avoid Varnish 503 bursts.
# Scaling: depth=3→20, depth=4→16, depth=5→12, depth=6+→8
# second scailing: giving depth 3→48, depth 4→44, depth 5→40 instead of 20/16/12.
# I checked this: python -c "import os; print(os.cpu_count())" 96
#CONCURRENT_TASKS = max(8, 20 - (MAX_DEPTH - 3) * 4)  
CONCURRENT_TASKS = max(24, 48 - (MAX_DEPTH - 3) * 4)  # more parallelism on high-core hardware
INTER_BATCH_DELAY = 0.0 if MAX_DEPTH <= 5 else 0.1  # seconds; light pacing between batches


# Number of parallel workers for STEP 8 result processing (Issue 3 fix).
# Each worker runs the CPU-heavy per-result pipeline (markdown extraction,
# table/image extraction, cleanup, file write) in a separate process.
# Set to 0 to disable parallelism and process results serially.
import os as _os
PARALLEL_WORKERS = min(64, _os.cpu_count() or 1)


# Maximum total pages to crawl (set to a large number for no practical limit)
# Set to a very large number (like 10000) to crawl all 862+ pages you found
# Or set a smaller number like 1000 to limit the crawl
# Note: BFSDeepCrawlStrategy doesn't accept None, so use a large number instead
# 10000 is effectively "no limit" for most sites
MAX_PAGES = 200000  # Increased to match reference file (100,012 URLs) with room for growth

# Anti-bot settings
ENABLE_STEALTH_MODE = False  # Disabled: honest crawler, not disguised as human browser
HEADLESS = True  # Run browser in headless mode (no visible window)

# Identifying User-Agent for DESY WebOffice logs
CRAWLER_USER_AGENT = (
    "Mozilla/5.0 (compatible; DESY-IT-LLM-Crawler/1.0; "
    "+https://it.desy.de/; contact taheri@mail.desy.de)"
)

# Per-page delay (ms) added inside CrawlerRunConfig to space requests within arun_many.
# Reduced from 500→100->10 ms; rate limiter already handles pacing. #should I decrease?
#PAGE_DELAY_MS = 100 if MAX_DEPTH >= 4 else 0
PAGE_DELAY_MS = 10 if MAX_DEPTH >= 4 else 0


# JavaScript rendering
# Crawl4AI uses Playwright by default, which handles JavaScript automatically
# This is already enabled - no additional config needed

# Timeout settings (in milliseconds)
# PERFORMANCE: 60s default; hanging pages release their slot faster.
# URLs that timeout will be logged in crawl_errors.json with timeout category
# Future runs can apply extended timeouts only to those specific URLs
PAGE_TIMEOUT = 120000    # 120 seconds - measured: it.desy.de responds in <0.5s raw; 12s = 24x buffer for Playwright overhead. Zero timeouts observed across all historical runs (18-24) even with the old 120s value.
PAGE_TIMEOUT_EXTENDED = 180000  # 180 seconds - for URLs that previously timed out

# Force consistent language for UI strings (prevents Indico language drift)
FORCE_LOCALE = "en-US"
ACCEPT_LANGUAGE_HEADER = "en-US,en;q=0.9"


# ============================================================================
# CRAWL CONFIGURATION DATACLASS
# ============================================================================
# Single source of truth for all crawl parameters.  Refactored phase-functions
# receive a CrawlConfig instance instead of reading module-level globals.
#
# The existing module-level constants above are kept for backward compatibility
# until every call-site is migrated.  The defaults here mirror those constants
# exactly so that ``CrawlConfig()`` reproduces the current behaviour.
# ============================================================================

@dataclass
class CrawlConfig:
    """All crawl configuration in one place."""

    # -- Seed URLs --------------------------------------------------------
    root_urls: list = field(default_factory=lambda: list(ROOT_URLS))

    # -- Scope restriction ------------------------------------------------
    allowed_url_prefixes: tuple = field(default_factory=lambda: ALLOWED_URL_PREFIXES)

    # -- Output paths -----------------------------------------------------
    output_dir: Path = field(default_factory=lambda: OUTPUT_DIR)
    log_dir: Path = field(default_factory=lambda: LOG_DIR)

    # -- Domain / redirect filtering --------------------------------------
    excluded_domains: set = field(default_factory=lambda: set(EXCLUDED_DOMAINS))
    check_redirects_to_excluded: bool = CHECK_REDIRECTS_TO_EXCLUDED

    # -- Checkpoint / resume ----------------------------------------------
    use_checkpoint: bool = USE_CHECKPOINT
    checkpoint_frequency: int = CHECKPOINT_FREQUENCY

    # -- GROUP 1: login / auth / admin patterns ---------------------------
    login_auth_admin_patterns: list = field(
        default_factory=lambda: list(LOGIN_AUTH_ADMIN_PATTERNS)
    )
    auth_only_domains: set = field(default_factory=lambda: set(AUTH_ONLY_DOMAINS))

    # -- GROUP 2: error-page patterns -------------------------------------
    error_url_patterns: list = field(
        default_factory=lambda: list(ERROR_URL_PATTERNS)
    )
    error_prone_domains: set = field(default_factory=lambda: set(ERROR_PRONE_DOMAINS))

    # -- GROUP 4: query-param dedup ---------------------------------------
    ui_only_query_params: set = field(default_factory=lambda: set(UI_ONLY_QUERY_PARAMS))
    content_critical_params: set = field(
        default_factory=lambda: set(CONTENT_CRITICAL_PARAMS)
    )

    # -- Crawler knobs ----------------------------------------------------
    concurrent_tasks: int = CONCURRENT_TASKS
    max_depth: int = MAX_DEPTH
    max_pages: int = MAX_PAGES
    enable_stealth_mode: bool = ENABLE_STEALTH_MODE
    headless: bool = HEADLESS
    page_timeout: int = PAGE_TIMEOUT
    page_timeout_extended: int = PAGE_TIMEOUT_EXTENDED
    force_locale: str = FORCE_LOCALE
    accept_language_header: str = ACCEPT_LANGUAGE_HEADER

    # -- Unified binary-extension list ------------------------------------
    # Superset of the old _BINARY_EXTENSIONS (pre-crawl) and
    # _BINARY_EXTENSIONS_CHECK (post-crawl).  One list, used everywhere.
    binary_extensions: tuple = (
        '.gif', '.jpg', '.jpeg', '.png', '.webp', '.svg', '.bmp', '.ico',
        '.tiff', '.tif', '.zip', '.tar', '.gz', '.rar',
        '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.flv', '.ogg', '.wav',
        # Academic / LaTeX source files (run-20 noise)
        '.tex', '.eps', '.ps', '.tgz', '.bib',
        '.f90', '.f77', '.f',
        '.bbl', '.blg', '.cls', '.sty', '.dtx', '.ins', '.aux',
        # RSS / Atom feeds
        '.atom',
    )

    # -- BFS exclusion patterns -------------------------------------------
    # Populated from the FilterChain setup inside crawl_site() when
    # URL_FILTER_AVAILABLE is True.  Defaults to [] so references in the
    # iterative link-extraction loop never raise NameError.
    exclusion_patterns: list = field(default_factory=list)

    # -- Derived (computed in __post_init__) -------------------------------
    error_log_file: Path = field(init=False, repr=False)
    checkpoint_file: Path = field(init=False, repr=False)
    compiled_login_patterns: list = field(init=False, repr=False)
    compiled_error_patterns: list = field(init=False, repr=False)

    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.error_log_file = self.log_dir / "crawl_errors.json"
        self.checkpoint_file = self.log_dir / "crawl_checkpoint.json"
        self.compiled_login_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.login_auth_admin_patterns
        ]
        self.compiled_error_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.error_url_patterns
        ]


# Default config instance — mirrors the module-level constants exactly.
config = CrawlConfig()


#6000=>148 errors
#45000=> 176 errors
# ============================================================================
# Custom Table Extraction Strategy (delegated to table_processing module)
LinkPreservingTableExtraction = _table_processing.LinkPreservingTableExtraction


def format_cell_with_links(cell_content, cell_html=None):
    """Delegate to table_processing."""
    return _table_processing.format_cell_with_links(cell_content, cell_html)


def is_pdf_url(url):
    """Delegate to url_utils."""
    return _url_utils.is_pdf_url(url)


def is_pubdb_url(url):
    """Delegate to table_processing."""
    return _table_processing.is_pubdb_url(url)


def is_pubdb_content(html_content):
    """Delegate to table_processing."""
    return _table_processing.is_pubdb_content(html_content)

_PUBDB_UI_KEYWORDS = _table_processing._PUBDB_UI_KEYWORDS

def is_pubdb_ui_table(table_text):
    """Delegate to table_processing."""
    return _table_processing.is_pubdb_ui_table(table_text)

def _is_pubdb_page(url, html_content):
    """Delegate to table_processing."""
    return _table_processing._is_pubdb_page(url, html_content)

# ============================================================================
# JavaScript-based Visibility Detection for Inactive/Hidden Links
# ============================================================================
# This script evaluates link visibility in the rendered browser, not just HTML.
# It detects:
# - Links hidden by CSS (display: none, visibility: hidden, opacity: 0)
# - Links in inactive/disabled containers
# - Links marked with ARIA disabled states
# - Links not in tab order (tabindex="-1")
# ============================================================================

def get_link_visibility_detection_script():
    """Delegate to content_extraction."""
    return _content_extraction.get_link_visibility_detection_script()

def is_indico_url(url):
    """Delegate to content_extraction."""
    return _content_extraction.is_indico_url(url)


def extract_indico_event(html_content, url=None):
    """Delegate to content_extraction."""
    return _content_extraction.extract_indico_event(html_content, url)


def _normalize_url(url):
    """Delegate to url_utils."""
    return _url_utils._normalize_url(url)


def _is_valid_crawl_url(url):
    """Delegate to url_utils (uses config.binary_extensions)."""
    return _url_utils._is_valid_crawl_url(url, config.binary_extensions)


# Redirect resolution cache lives in url_utils; alias for backward compat.
_redirect_resolution_cache = _url_utils._redirect_resolution_cache


async def _resolve_redirect_final_host(url, timeout=5):
    """Delegate to url_utils."""
    return await _url_utils._resolve_redirect_final_host(url, timeout)


def _normalize_domain(netloc):
    """Delegate to url_utils."""
    return _url_utils._normalize_domain(netloc)


def _is_desy_domain(netloc):
    """Delegate to url_utils."""
    return _url_utils._is_desy_domain(netloc)


def _is_allowed_crawl_url(url):
    """Delegate to url_utils (uses config.allowed_url_prefixes)."""
    return _url_utils._is_allowed_crawl_url(url, config.allowed_url_prefixes)


def should_skip_login_auth_url(url):
    """Delegate to url_utils (uses config compiled patterns & auth domains)."""
    return _url_utils.should_skip_login_auth_url(
        url, config.compiled_login_patterns, config.auth_only_domains
    )


def should_skip_error_url(url):
    """Delegate to url_utils (uses config compiled patterns & error domains)."""
    return _url_utils.should_skip_error_url(
        url, config.compiled_error_patterns, config.error_prone_domains
    )


def normalize_url_for_dedup(url):
    """Delegate to url_utils (uses config query-param sets)."""
    return _url_utils.normalize_url_for_dedup(
        url, config.ui_only_query_params, config.content_critical_params
    )


def should_skip_query_param_duplicate(url, seen_normalized_urls):
    """Delegate to url_utils (uses config query-param sets)."""
    return _url_utils.should_skip_query_param_duplicate(
        url, seen_normalized_urls,
        config.ui_only_query_params, config.content_critical_params
    )


def _is_empty_or_whitespace(text):
    """Delegate to table_processing."""
    return _table_processing._is_empty_or_whitespace(text)


def _is_in_navigation(elem):
    """Delegate to table_processing."""
    return _table_processing._is_in_navigation(elem)


def _is_separator_line(line):
    """Delegate to markdown_cleanup."""
    return _markdown_cleanup._is_separator_line(line)


def _normalize_text_spacing(line):
    """Delegate to markdown_cleanup."""
    return _markdown_cleanup._normalize_text_spacing(line)


def extract_external_links(html_content, current_url):
    """Delegate to content_extraction."""
    return _content_extraction.extract_external_links(html_content, current_url)


def extract_cell_links(cell_element):
    """Delegate to table_processing."""
    return _table_processing.extract_cell_links(cell_element)


def enrich_crawl4ai_tables_with_links(result, is_pdf=False):
    """Delegate to table_processing."""
    return _table_processing.enrich_crawl4ai_tables_with_links(result, is_pdf)


def enrich_table_with_html_links(crawl_table, html_table):
    """Delegate to table_processing."""
    return _table_processing.enrich_table_with_html_links(crawl_table, html_table)


def extract_table_from_html(html_table):
    """Delegate to table_processing."""
    return _table_processing.extract_table_from_html(html_table)


def parse_single_column_cell_html(cell_element):
    """Delegate to table_processing."""
    return _table_processing.parse_single_column_cell_html(cell_element)


def normalize_field_label(label):
    """Delegate to table_processing."""
    return _table_processing.normalize_field_label(label)


def parse_single_column_table_content(cell_html):
    """Delegate to table_processing."""
    return _table_processing.parse_single_column_table_content(cell_html)


def convert_single_column_to_multi_column_table(table_data, html_table_element):
    """Delegate to table_processing."""
    return _table_processing.convert_single_column_to_multi_column_table(table_data, html_table_element)


def extract_headings_and_tables_in_dom_order(html_content, url=None):
    """Delegate to table_processing."""
    return _table_processing.extract_headings_and_tables_in_dom_order(html_content, url)


def format_tables_with_headings_as_markdown(dom_ordered_content):
    """Delegate to table_processing."""
    return _table_processing.format_tables_with_headings_as_markdown(dom_ordered_content)

def inject_links_into_markdown_tables(markdown_content, html_content):
    """Delegate to table_processing."""
    return _table_processing.inject_links_into_markdown_tables(markdown_content, html_content)


def format_table_markdown_inline(table):
    """Delegate to table_processing."""
    return _table_processing.format_table_markdown_inline(table)


def get_table_header_normalized(formatted_table):
    """Delegate to table_processing."""
    return _table_processing.get_table_header_normalized(formatted_table)

# ============================================================================
# Enhanced Duplication Detection and Noise Removal Functions
# ============================================================================

def normalize_text_enhanced(text):
    """Delegate to content_extraction."""
    return _content_extraction.normalize_text_enhanced(text)


def normalize_markdown_links(text):
    """Delegate to content_extraction."""
    return _content_extraction.normalize_markdown_links(text)


def extract_emails_from_text(text):
    """Delegate to content_extraction."""
    return _content_extraction.extract_emails_from_text(text)


def text_similarity(text1, text2):
    """Delegate to content_extraction."""
    return _content_extraction.text_similarity(text1, text2)


def detect_enhanced_repetition(markdown_lines):
    """Delegate to content_extraction."""
    return _content_extraction.detect_enhanced_repetition(markdown_lines)


def extract_contact_blocks(html_soup):
    """Delegate to content_extraction."""
    return _content_extraction.extract_contact_blocks(html_soup)


def reconstruct_contact_structure(contact_blocks, page_title=None):
    """Delegate to content_extraction."""
    return _content_extraction.reconstruct_contact_structure(contact_blocks, page_title)


def clean_markdown_links_post_process(markdown_text):
    """Delegate to content_extraction."""
    return _content_extraction.clean_markdown_links_post_process(markdown_text)



# PHASE 2 FIX: Removed domain-specific .inactive filtering
    # Now .inactive selectors are applied universally to all domains
    # This ensures consistent behavior across all 200k+ URLs without domain whitelisting


from collections import namedtuple

CrawlSetup = namedtuple('CrawlSetup', [
    'browser_config',
    'deep_crawl_strategy',
    'exclusion_patterns',
    'excluded_selector_str',
    'inactive_selectors',
    'markdown_generator',
    'table_extraction_strategy',
    'dedup_enabled',
])

def _build_crawl_setup():
    """Build browser config, URL filters, crawl strategy, and content pipeline.

    Centralises Steps 2-4 of crawl_site() — everything that creates
    configuration objects for the crawl run.  Returns a CrawlSetup namedtuple.
    """
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

        # Honest identifying User-Agent so DESY WebOffice logs show the crawler identity
        user_agent=CRAWLER_USER_AGENT,

        # text_mode: Disable loading images, fonts, CSS, and media at the Chromium level.
        # We only extract text/markdown/links, so there is zero content loss.
        # Reduces page load time and memory usage.
        text_mode=True,
        
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
            # SOLUTION 2: Add binary file filters
            r'.*\.ps(\?.*)?$',   # PostScript files
            r'.*\.ps\.gz(\?.*)?$', # Compressed PostScript files
            r'.*\.mov(\?.*)?$',  # QuickTime video files
            r'.*\.pptx?(\?.*)?$', # PowerPoint presentations (.ppt and .pptx)
            r'.*\.dotx(\?.*)?$', # Word templates
            r'.*\.wmv(\?.*)?$',  # Windows Media Video
            r'.*\.tar\.gz(\?.*)?$', # Compressed tar archives
            r'.*\.gz(\?.*)?$',   # Generic compressed files (if not already covered)
            # Filter non-HTTP protocols to prevent "Invalid URL" errors from crawl4ai
            r'^callto:.*',       # Phone number links (callto:)
            r'^davs?:.*',        # WebDAV links (davs:://, dav:://)
            r'^mattermost:.*',   # Mattermost chat links
            r'.*\.mobileconfig(\?.*)?$', # mobile configuration files
            r'.*\.pem(\?.*)?$', # pem files (certificates)
            r'^file:.*',         # File protocol
            r'^ftp:.*',          # FTP protocol
            r'^tel:.*',          # Telephone protocol
            r'^mailto:.*',       # Email links (already handled, but filter from crawling)
            r'^javascript:.*',   # JavaScript pseudo-protocol
            r'^data:.*',         # Data URIs
            r'^(?!https?://).*://.*',  # Any protocol that's not http:// or https://
            # Malformed / non-HTTP schemes (crawler_19813672.err)
            r'^https?://\s*$',   # Empty http(s) (http:// with no host)
            r'^doi:.*',          # DOI links
            r'^ttps://.*',       # Typo: ttps instead of https
            r'^urn:.*',          # URN (not HTTP URL)
            # Exclude specific DESY subdomains from crawling at all depth levels
            r'^https?://(www\.)?fater\.desy\.de/.*',
            r'^https?://(www\.)?bib-pubdb1\.desy\.de/.*',
            r'^https?://(?!it\.desy\.de[/]).*\.desy\.de',  # Block all non-it.desy.de DESY subdomains (BFS eTLD+1 fix)
            # Exclude repeated news pages with no content and many links causing crawler to get stuck
            r'^https?://[^/]+/(?:e\d+/){3,}',  # Block 3+ event ID nesting
            
            # ====================================================================
            # PATH A FIX: GROUP 1 - LOGIN/AUTH/ADMIN URL FILTERING (PRE-CRAWL)
            # ====================================================================
            # These patterns are now applied BEFORE BFSDeepCrawlStrategy crawls URLs
            # Previously defined at lines 238-260 but only used post-crawl (too late)
            # Now BFS will skip these URLs entirely, saving bandwidth and time
            # Impact: ~490 login pages prevented (vs folder 14 which has 490 login files)
            r'.*/login(/|$|\?|\.php|\.jsp)', # /login, /login/, /login?, /login.php, /login.jsp
            r'.*/login_form',              # /login_form anywhere in URL
            r'.*/auth\.view',              # /auth.view (ADONIS systems)
            r'.*/auth(/|$|\?)',            # /auth, /auth/, /auth? (NOT 'authors!')
            r'.*login\.jsp',               # Any login.jsp (covers aclogin.jsp etc)
            r'.*/authenticate',            # /authenticate anywhere
            r'.*/authorization',           # /authorization anywhere
            r'.*/signin',                  # /signin anywhere
            r'.*/register(/|$|\?)',        # /register, /register/, /register?...
            r'.*/enrollment',              # /enrollment anywhere
            r'.*/acl_users',               # Zope /acl_users (login system)
            r'.*/admin(/|$|\?)',           # /admin, /admin/, /admin?...
            r'.*/manage(/|$|\?)',          # /manage, /manage/, /manage?...
            r'.*/edit_entry\.php',         # /edit_entry.php (booking systems)
            r'.*/selfregistration',        # /selfregistration anywhere
            r'.*/pls/apex',                # Oracle APEX login pages
            r'.*/accounts(/|$|\?)',        # /accounts, /accounts/, /accounts?...
            r'.*\?destination=',           # Query param indicating redirect-after-login
            r'.*\?came_from=',             # Query param indicating redirect-after-login
            r'.*[?&]login',                # Query param containing 'login'
            r'.*[?&]manage',               # Query param containing 'manage'
            # Auth-only domains (entire domain serves only login pages)
            r'^https?://(www\.)?selfregistration\.desy\.de/.*',
            r'^https?://(www\.)?sso\.desy\.de/.*',
            r'^https?://(www\.)?idp\.desy\.de/.*',
            r'^https?://(www\.)?keycloak\.desy\.de/.*',
            
            # ====================================================================
            # PATH A FIX: GROUP 2 - ERROR PAGES FILTERING (PRE-CRAWL)
            # ====================================================================
            # Skip URLs that typically return error pages (404, 503, maintenance)
            # Previously defined at lines 284-295 but only used post-crawl (too late)
            # Impact: ~46 error pages prevented (vs folder 14 which has 46 error files)
            r'.*/error(/|$|\?)',           # /error, /error/, /error?...
            r'.*/404(/|$|\?|\.)',          # /404, /404/, /404?, /404.html
            r'.*/403(/|$|\?|\.)',          # /403, /403/, /403?, /403.html
            r'.*/500(/|$|\?|\.)',          # /500, /500/, /500?, /500.html
            r'.*/503(/|$|\?|\.)',          # /503, /503/, /503?, /503.html
            r'.*/maintenance(/|$|\?)',     # /maintenance anywhere
            r'.*/unavailable(/|$|\?)',     # /unavailable anywhere
            r'.*/offline(/|$|\?)',         # /offline anywhere
            r'.*/notfound(/|$|\?)',        # /notfound anywhere
            r'.*\.error(/|$|\?)',          # .error file extension
            r'.*/cgi-bin/',                # Legacy CGI scripts often error
            # Error-prone domains
            r'^https?://(www\.)?old\.desy\.de/.*',
            r'^https?://(www\.)?legacy\.desy\.de/.*',

            # ====================================================================
            # FIX (Problem 6 & 7): GROUP 3b — RSS/FEED PAGES AND LOGOFF URLS (PRE-CRAWL)
            # ====================================================================
            # RSS/Atom endpoint URLs and logoff action URLs produce no useful prose content.
            # Block them at BFS link-queue stage so they are never fetched.
            r'.*/rss(/|$|\?)',             # /rss, /rss/, /rss? — RSS feed endpoints
            r'.*/feed(/|$|\?)',            # /feed, /feed/, /feed? — generic feed endpoints
            r'.*[?&]logoff',               # ?logoff=t and similar session-termination params
            r'.*/logout(/|$|\?)',          # /logout, /logout/, /logout? (belt+suspenders)

            # ====================================================================
            # FIX (Problem 1): GROUP 3 — PRINTVERSION / SITEVIEW DEDUPLICATION (PRE-CRAWL)
            # ====================================================================
            # Plone CMS exposes two URL surfaces for a print-optimised copy of every page:
            #   1.  /@@siteview            — the Plone traversal view (any path segment)
            #   2.  ?printversion=1        — legacy query-param variant
            # Both surfaces render navigation menus only — zero unique body content.
            # Adding these patterns here blocks them at the BFS link-queue stage so
            # they are never fetched at all, complementing the post-crawl
            # normalize_url_for_dedup() check which only deduplicates after fetching.
            r'.*/@@siteview(\?.*)?$',      # Plone @@siteview traversal view
            r'.*[?&]printversion=[^&]*',   # ?printversion=1 or &printversion=1

            # ====================================================================
            # FIX: GROUP 4 — CALENDAR / CMS-RAW / BROKEN-LINK FILTERING (PRE-CRAWL)
            # ====================================================================
            # Zimbra calendar event popups: each month-view page spawns ~16 invId
            # links.  At depth 8 these will multiply explosively.
            r'.*[?&]invId=',               # Zimbra individual event popup
            r'.*[?&]instDuration=',         # Zimbra event duration param (always with invId)
            # CMS internal raw-content paths (agenda/protokoll .txt files)
            r'.*/sites2009/',               # Plone CMS raw content tree
            # CMS preview mode (renders same content with preview chrome)
            r'.*[?&]preview=preview',       # ?preview=preview or &preview=preview
            # Broken internal-link error pages (op=not_found&url=…)
            r'.*[?&]op=not_found',          # CMS broken-link landing pages
        ]
        
        # Create filter list first
        filter_list = []
        # Prepend (?i) to each pattern so extensions like .PNG/.PDF/.JPG are
        # caught in addition to their lowercase variants.
        ci_exclusion_patterns = [f'(?i){p}' for p in exclusion_patterns]
        if URL_FILTER_TYPE == 'RegexURLFilter':
            # RegexURLFilter API: (pattern, include=False) to exclude
            filter_list = [URL_FILTER_CLASS(pattern, include=False) for pattern in ci_exclusion_patterns]
        elif URL_FILTER_TYPE == 'URLPatternFilter':
            # URLPatternFilter API: (patterns=[...], reverse=True) to exclude
            # reverse=True means exclude URLs matching the patterns
            filter_list = [URL_FILTER_CLASS(patterns=ci_exclusion_patterns, reverse=True)]
        
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
        
        # PATH A STATUS: Log that pre-crawl filtering is now active
        print(f"[PATH A] ✅ Pre-crawl URL filtering ACTIVE: {len(exclusion_patterns)} patterns")
        print(f"[PATH A] GROUP 1 (login/auth): ~25 patterns will prevent crawling login pages")
        print(f"[PATH A] GROUP 2 (error pages): ~12 patterns will prevent crawling error pages")
        print(f"[PATH A] GROUP 3 (printversion/siteview): 2 patterns will block @@siteview and ?printversion= URLs")
        print(f"[PATH A] GROUP 3b (rss/feed/logoff): 4 patterns will block RSS, feed, and logoff URLs")
        print(f"[PATH A] BFSDeepCrawlStrategy will skip matching URLs BEFORE crawling them")
    else:
        # URL filter classes not importable — BFS runs without filter_chain.
        # Subdomains and saved results are already guarded by link_domain==base_domain
        # (manual BS loop) and _is_allowed_crawl_url / should_skip_login_auth_url
        # (filter_result_pre_save).  The only real gap is the manual BS loop re-queuing
        # /manage, /admin, /login paths before filter_result_pre_save sees them.
        # Populate a minimal exclusion_patterns so _exclusion_re blocks those re-queues.
        exclusion_patterns = [
            r'.*/manage(/|$|\?)',          # Zope/Plone /manage admin paths
            r'.*[?&]manage',               # ?manage / &manage query params
            r'.*/admin(/|$|\?)',           # /admin paths
            r'.*/acl_users',               # Zope ACL
            r'.*/login(/|$|\?|\.php|\.jsp)',  # login pages
            # GROUP 4 — Calendar / CMS-raw / broken-link (mirror of PATH A)
            r'.*[?&]invId=',               # Zimbra calendar event popup
            r'.*[?&]instDuration=',         # Zimbra event duration param
            r'.*/sites2009/',               # Plone CMS raw content tree
            r'.*[?&]preview=preview',       # CMS preview mode
            r'.*[?&]op=not_found',          # CMS broken-link landing pages
        ]
        filter_chain = None
        print("[PATH A] ⚠ URL filter classes not available — BFS pre-crawl filtering DISABLED")
    
    # DOMAIN RESTRICTION: BFSDeepCrawlStrategy's include_external=False only allows exact domain match.
    # Additional manual extraction paths below are restricted by ALLOWED_URL_PREFIXES.
    
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
    # MOVED: PruningContentFilter initialization moved AFTER excluded_selectors definition
    # (see line ~5470 after excluded_selectors is defined at line ~5270)
    markdown_generator = None
    
    # Layer 2: Explicit CSS selectors for visually-hidden and navigation content
    # This ensures navigation, footers, and hidden elements are excluded even if PruningContentFilter misses them
    # We exclude both truly hidden elements AND common navigation/footer patterns
    # ENHANCED: Now includes disabled/inactive link detection for pages with inactive menus
    excluded_selectors = [
        # ========================================================================
        # SECTION 0: CRITICAL - Inactive/Disabled Elements (News Sidebars)
        # ========================================================================
        # These selectors block news sidebars and disabled navigation items
        # that appear globally across all pages (e.g., news archive links)
        # Pattern found: <li class="inactive ZMSDocument0"><a href="...">News</a></li>
        '.inactive',                  # Inactive class (universal DESY pattern)
        '[class*="inactive"]',       # Any class containing "inactive"
        'a.inactive',                 # Links directly marked .inactive
        'li.inactive',                # List items with .inactive
        'li[class*="inactive"]',     # List items with "inactive" in class
        '[class*="ZMSDocument"][class*="inactive"]', # ZMS CMS items marked inactive
        
        # ========================================================================
        # SECTION 1: Visually hidden elements
        # ========================================================================
        '.visually-hidden',
        '[aria-hidden="true"]',
        '[hidden]',
        '[style*="display: none"]',
        '[style*="visibility: hidden"]',
        '[style*="opacity: 0"]',
        '[style*="height: 0"]',
        '[style*="width: 0"]',
        '[aria-expanded="false"]',
        
        # ========================================================================
        # SECTION 2: Disabled/Inactive links and menu items - OPTIMIZED
        # ========================================================================
       
        'a[disabled]',                # Links with disabled attribute
        'button[disabled]',           # Disabled buttons
        '[role="button"][disabled]',  # Disabled elements with button role
        'a[aria-disabled="true"]',    # ARIA disabled state on links
        'button[aria-disabled="true"]', # ARIA disabled state on buttons
        '[aria-disabled="true"]',     # Any element with ARIA disabled
        'a[tabindex="-1"]',           # Links not in tab order (often inactive)
        
        # Links/menu items in disabled containers
        '.disabled a',                # Links inside disabled container
        '.disabled',                  # Disabled class anywhere
        '[class*="disabled"] a',      # Links in any class with "disabled"
        '[id*="disabled"]',           # Elements with disabled in ID
        'a.disabled',                 # Direct disabled class on links
        'a[class*="disabled"]',       # Links with disabled in their class
        'a[class*="no-click"]',       # Links marked as non-clickable
        'a[class*="no-link"]',        # Links marked as non-link
        
        # Menu items marked inactive
        'li[class*="disabled"]',      # List items with disabled class
        'li[aria-disabled="true"]',   # List items with ARIA disabled
        'li[role="menuitem"][aria-disabled]', # Menu items marked disabled
        '[role="menuitem"][aria-disabled="true"]', # Menu items disabled via ARIA
        '[role="option"][aria-disabled="true"]',   # Options disabled via ARIA
        
        # Special DESY patterns (add based on inspection)
        '[class*="is-disabled"]',     # Active state pattern (is-* prefix)
        '[class*="is-inactive"]',     # Inactive state pattern
        
        # ========================================================================
        # DESY ZMS CMS Inactive Menu Items (particle-physics.desy.de)
        # ========================================================================
        # Targets CMS folder and document items marked as inactive
        # The .inactive selector above catches these, but made explicit here for clarity
        # '[class*="ZMSFolder"]',       # CMS folder items (ZMSFolder0-N) when combined with inactive
        # '[class*="ZMSDocument"]',     # CMS document items (ZMSDocument0-N) when combined with inactive
        # 'a[class*="ZMSFolder"]',      # Links within ZMS folder items
        # 'a[class*="ZMSDocument"]',    # Links within ZMS document items
        
        # ========================================================================
        # SECTION 3: Navigation and menus - OPTIMIZED (removed overly broad selectors)
        # ========================================================================
        # REMOVED: [class*="nav"], [id*="nav"], [class*="menu"], [id*="menu"]
        # REASON: Too risky - blocks legitimate navigation content on many sites
        # KEPT: Semantic HTML elements (nav, header, footer, aside)
        'nav',                        # HTML5 nav element
        '.navbar',                    # Bootstrap navbar
        '[role="navigation"]',        # ARIA navigation role
        
        # ========================================================================
        # SECTION 4: Footers and headers - OPTIMIZED
        # ========================================================================
        # REMOVED: Footer/header class variants ([class*="footer"], etc.)
        # KEPT: Semantic HTML elements
        'footer',                     # HTML5 footer element
        'header',                     # HTML5 header element
        
        # ========================================================================
        # SECTION 5: Sidebars and aside elements
        # ========================================================================
        'aside',                      # HTML5 aside element
        '.sidebar',                   # Sidebar class
        
        # ========================================================================
        # SECTION 6: Cookie and privacy notices
        # ========================================================================
        '.cookie',                    # Cookie consent notice
        '.privacy',                   # Privacy notice
        
        # ========================================================================
        # SECTION 9: Non-HTTP / malformed links (avoid crawl4ai "Invalid URL" warnings)
        # ========================================================================
        # Only keep most common malformed patterns - removed rare ones
        'a[href^="tel:"]',            # Telephone links
        'a[href^="dav:"]',            # WebDAV links
        'a[href^="mailto:"]',         # Mailto links
        'a[href^="callto:"]',         # Skype/VoIP links
        'a[href^="urn:"]',            # URN links
    ]
    
    # ========================================================================
    # PHASE 4 OPTIMIZATION SUMMARY
    # ========================================================================
    # Removed selectors (saving ~50% matching overhead):
    # - [class*="nav"], [id*="nav"]: Too broad, blocks nav content
    # - [class*="menu"], [id*="menu"]: Too broad
    # - [class*="footer"], [id*="footer"]: Redundant with 'footer'
    # - [class*="header"], [id*="header"]: Redundant with 'header'
    # - [class*="sidebar"], [id*="sidebar"]: Redundant with '.sidebar'
    # - [class*="cookie"], [id*="cookie"]: Rarely needed
    # - [class*="loading"], [id*="loading"]: Skip links rarely have crawlable content
    # - [class*="placeholder"]: Animation elements, not important content
    # - .skip-link, .sprungnavigation, [class*="skip"]: Skip links, not important
    # - [id*="breadcrumb"]: Rare
    # - .impressum, .datenschutz: German-specific, can use .privacy
    #
    # Kept essential selectors (35 total):
    # - All disabled/inactive patterns (semantic meaning)
    # - Hidden CSS styles (display:none, visibility:hidden, etc.)
    # - Semantic HTML elements (nav, footer, header, aside)
    # - Critical class patterns (.navbar, .sidebar, .cookie, .privacy)
    # - Malformed URL patterns (tel:, mailto:, dav:, etc.)
    #
    # Expected impact: +15% crawl speed, <1% content loss
    # Risk: Low - removed patterns rarely contain crawlable content
    # ========================================================================
    
    # ========================================================================
    # DOMAIN-SPECIFIC: .inactive selectors (applies universally to all URLs)
    # ========================================================================
    # These selectors filter links in sections marked .inactive.
    # Skip for cms.desy.de where .inactive is part of structural class naming.
    # 
    # Strategy: Will be applied conditionally per URL during crawl,
    # not globally during selector string building.
    INACTIVE_SELECTORS = [
        '.inactive a',                # Links in inactive sections
        '.inactive',                  # Inactive class anywhere
        '[class*="inactive"] a',      # Links in any class with "inactive"
        '[id*="inactive"]',           # Elements with inactive in ID
        'a.inactive',                 # Direct inactive class on links
        'a[class*="inactive"]',       # Links with inactive in their class
        'li[class*="inactive"]',      # List items with inactive class
    ]
    
    excluded_selector_str = ', '.join(excluded_selectors)
    
    # GROUP 6 OPTIMIZATION: Initialize PruningContentFilter with excluded_selectors
    # Now that excluded_selectors is defined, we can use it
    if PRUNING_FILTER_AVAILABLE:
        # Lower threshold (0.2 instead of 0.5) to be less aggressive
        # This preserves more content including lists and short structured blocks
        # min_word_threshold=1 allows single-word list items to be retained
        try:
            prune_filter = PruningContentFilter(
                threshold=0.2,              # Lower threshold = less aggressive filtering
                threshold_type="dynamic",  # Dynamic threshold adapts to content
                min_word_threshold=1,      # Allow single-word items (important for lists)
                excluded_selectors=excluded_selectors  # GROUP 6: Remove .inactive, hidden, disabled content
            )
            print("[GROUP 6] ✅ PruningContentFilter initialized with excluded_selectors")
            print(f"[GROUP 6]    Filtering {len(excluded_selectors)} CSS selectors for inactive/disabled/hidden content")
        except TypeError:
            # If excluded_selectors parameter not supported in this version of crawl4ai,
            # fall back to standard initialization without it. We'll implement Option B (BeautifulSoup) instead.
            print("[GROUP 6] ⚠️  WARNING: excluded_selectors not supported by PruningContentFilter")
            print("[GROUP 6]    Falling back to standard filter initialization (will implement BeautifulSoup fallback)")
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
            verbose=False #True               # Enable logging for debugging
        )
        print(f"[INFO] Link-preserving table extraction enabled for HTML and PDF pages")
    elif TABLE_EXTRACTION_AVAILABLE:
        # Fallback to DefaultTableExtraction if custom strategy not available
        table_extraction_strategy = DefaultTableExtraction(
            table_score_threshold=1,  # Very low threshold to catch all tables (default is 7, lower = more tables)
            min_rows=1,                # Minimum rows (allow single-row tables)
            min_cols=1,                # Minimum columns (allow single-column tables like card layouts)
            verbose=False #True               # Enable logging for debugging
        )
        print(f"[INFO] Table extraction enabled (links may not be preserved)")
    
    # Info: Invisible link detection available via _content_extraction.get_invisible_link_urls()
    print("[INFO] Invisible link detection enabled - will skip inactive menu links (display:none, disabled, etc.)")
    
    # ========================================================================
    # FEATURE #3: Helper function - Extract links early before crawling
    # ========================================================================
    # ========================================================================
    # FEATURE #4: Content Deduplication using ContentHash
    # ========================================================================
    # PHASE 6 FIX: Detect and skip pages with duplicate content
    # This automatically identifies pages with identical navbar/sidebar
    # (solves the Sara Taheri issue where duplicates accumulate)
    # Benefits:
    # - Prevents accumulating identical content
    # - Automatically detects duplicate pages (exact content match)
    # - Faster crawl by skipping redundant pages
    # - Solves Sara Taheri duplication problem directly
    
    dedup_enabled = CONTENT_HASH_AVAILABLE
    
    # Content deduplication and HTML filtering available via _content_extraction module
    
    # Info: Content deduplication is available
    if CONTENT_HASH_AVAILABLE:
        print("[INFO] ContentHash enabled - will detect and skip duplicate content (page-level deduplication)")

    return CrawlSetup(
        browser_config=browser_config,
        deep_crawl_strategy=deep_crawl_strategy,
        exclusion_patterns=exclusion_patterns,
        excluded_selector_str=excluded_selector_str,
        inactive_selectors=INACTIVE_SELECTORS,
        markdown_generator=markdown_generator,
        table_extraction_strategy=table_extraction_strategy,
        dedup_enabled=dedup_enabled,
    )


def _print_crawl_summary(state, dedup_enabled, start_time=None):
    """Print final crawl statistics (extracted from crawl_site for readability)."""
    import time as _time
    print("-" * 60)
    print(f"[SUMMARY]")
    print(f"  URLs processed: {len(ROOT_URLS)}")
    print(f"  Successful: {len(state.all_successful_urls)} pages")
    print(f"  Errors: {len(state.all_errors)} pages")
    print(f"  Total crawled: {state.total_results_crawled} pages")
    if start_time is not None:
        total_s = _time.time() - start_time
        h, rem = divmod(int(total_s), 3600)
        m, s = divmod(rem, 60)
        print(f"  Total run time: {h:02d}h {m:02d}m {s:02d}s  ({total_s:.1f}s)")
        n = state.total_results_crawled or len(state.all_successful_urls)
        if n > 0:
            print(f"  Avg time/page:  {total_s / n:.2f}s")

    # GROUP 1: Report login/auth/admin filtering statistics
    print("-" * 60)
    print(f"[GROUP 1 STATISTICS]")
    print(f"  URLs skipped (login/auth/admin): {state.group1_skipped_count}")
    total_queue_urls = (len(state.all_successful_urls) + state.group1_skipped_count
                        + state.group2_skipped_count
                        + (len(state.all_errors) if state.all_errors else 0))
    if total_queue_urls > 0:
        group1_reduction_pct = (state.group1_skipped_count / total_queue_urls) * 100
        print(f"  Reduction: {group1_reduction_pct:.1f}%")

    # GROUP 2: Report error page filtering statistics
    print("-" * 60)
    print(f"[GROUP 2 STATISTICS]")
    print(f"  URLs skipped (error/maintenance): {state.group2_skipped_count}")
    if total_queue_urls > 0:
        group2_reduction_pct = (state.group2_skipped_count / total_queue_urls) * 100
        print(f"  Reduction: {group2_reduction_pct:.1f}%")

    # GROUP 4: Report query parameter deduplication statistics
    print("-" * 60)
    print(f"[GROUP 4 STATISTICS]")
    print(f"  URLs skipped (query param duplicates): {state.group4_skipped_count}")
    if total_queue_urls > 0:
        group4_reduction_pct = (state.group4_skipped_count / total_queue_urls) * 100
        print(f"  Reduction: {group4_reduction_pct:.1f}%")
    if state.group4_skipped_count > 0:
        print(f"  Note: These URLs had identical content with only query param variations")
        print(f"        (e.g., ?printversion=1 vs ?embed=1 - prevented redundant crawls)")

    # Combined filtering statistics
    print("-" * 60)
    total_filtered = state.group1_skipped_count + state.group2_skipped_count + state.group4_skipped_count
    print(f"[COMBINED FILTERING (GROUP 1 + GROUP 2 + GROUP 4)]")
    print(f"  Total URLs filtered: {total_filtered}")
    if total_queue_urls > 0:
        combined_reduction_pct = (total_filtered / total_queue_urls) * 100
        print(f"  Combined reduction: {combined_reduction_pct:.1f}%")
        print(f"  Crawl efficiency: {(len(state.all_successful_urls) / total_queue_urls) * 100:.1f}% of URLs were useful")

    if dedup_enabled:
        print(f"  Duplicates skipped: {state.duplicate_count} pages (identical content)")
        print(f"  Unique content: {len(state.seen_content_hashes)} pages")
    print(f"  Files saved to: {OUTPUT_DIR}/")
    if state.all_errors:
        print(f"  Error log: {ERROR_LOG_FILE}")
    print("-" * 60)



def _resolve_markdown_content(result, result_is_pdf):
    """Select best markdown from fit/raw, falling back to HTML extraction.

    Encapsulates Steps G6-G7 of crawl_site(): fit-vs-raw markdown selection,
    table detection, HTML fallback via BeautifulSoup (link conversion, contact
    block merging, paragraph extraction, noise filtering, dedup), and
    full-page last-resort extraction.

    Returns the resolved markdown content string (may be empty).
    """
    markdown_content = ""
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
                        # PERFORMANCE FIX: Use cached soup to avoid redundant parsing
                        soup_probe = _get_cached_soup(result)
                        probe_tables = soup_probe.find_all('table', recursive=True) if soup_probe else []
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
                            # PERFORMANCE FIX: Use cached soup to avoid redundant parsing
                            soup = _get_cached_soup(result)
                            if not soup:
                                raise ValueError('No soup')


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
                                        # FIX 1b: Collect filtered paragraphs in ONE pass.
                                        # The original code applied the same filter twice — once to build
                                        # paragraph_texts (for the length check), then again to build para_soup.
                                        # Now we iterate all_paragraphs once, store the elements, and reuse them.
                                        filtered_paras = []
                                        for para in all_paragraphs:
                                            para_text = para.get_text(strip=True)
                                            # Check if paragraph has contact info or substantial content
                                            if para_text and (len(para_text) > 20 or re.search(r'@|mailto:|T\.\s*\(?\d+|\(he/him\)|\(she/her\)', para_text)):
                                                # Skip if it's clearly navigation/noise
                                                if para_text.count('](') < 10 and not re.match(r'^https?://', para_text):
                                                    filtered_paras.append(para)
                                        paragraph_texts = [p.get_text(strip=True) for p in filtered_paras]

                                        if paragraph_texts:
                                            para_joined = '\n'.join(paragraph_texts)
                                            if len(para_joined) > len(main_content_text_preview):
                                                # Use paragraphs instead
                                                print(f"[DEBUG] Using {len(paragraph_texts)} paragraphs for extraction (total {len(para_joined)} chars)")
                                            # Create a temporary soup with just these paragraphs
                                            para_soup = BeautifulSoup('', 'lxml')
                                            for para in filtered_paras:
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
                                                # This is a contact info block - replace the entire <a> tag with a
                                                # paragraph containing the formatted contact block text.
                                                # FIX 1c: The original code mutated text nodes inside the link
                                                # (replacing the email string with a markdown link), but then
                                                # immediately replaced the entire link element via link.replace_with(),
                                                # discarding all those DOM mutations. The email is already replaced
                                                # correctly below via re.sub() on link_content (derived from
                                                # link_full_text). The wasted text_node loop is removed.
                                                email_pattern = re.escape(email)

                                                # GENERAL: Replace link with a paragraph containing all its text content
                                                # This ensures the contact block is extracted as a single paragraph
                                                link_content = link_full_text
                                                # Replace email with markdown link
                                                link_content = re.sub(f'\\b{email_pattern}\\b', f'[{email}](mailto:{email})', link_content)
                                                # Create a new paragraph element (Fix #2: use callable() guard - hasattr passes for NavigableString
                                                # because new_tag exists as an attribute but is None, causing TypeError on call)
                                                tag_parent = main_content if main_content is not None else soup
                                                if tag_parent is not None and callable(getattr(tag_parent, 'new_tag', None)):
                                                    new_para = tag_parent.new_tag('p')
                                                    new_para['class'] = 'contact-block-extracted'
                                                    new_para.string = link_content
                                                    link.replace_with(new_para)
                                                else:
                                                    link.replace_with(NavigableString(link_content))
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
                                    main_content_for_text = BeautifulSoup(str(main_content), 'lxml')

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

    return markdown_content


def _final_memory_cleanup(state, results_metadata):
    """Save final checkpoint and aggressively free memory after processing."""
    print(f"[MEMORY] Starting final cleanup after processing {state.pages_processed} pages...")
    print(f"[MEMORY] Preserved lightweight metadata for {len(results_metadata)} results (heavy data freed)")
    total_html_bytes = sum(m.get('html_length', 0) for m in results_metadata)
    total_md_bytes = sum(m.get('markdown_length', 0) for m in results_metadata)
    print(f"[MEMORY] Freed approximately {(total_html_bytes + total_md_bytes) / (1024*1024):.1f} MB of HTML+markdown data")
    final_checkpoint_data = state.to_checkpoint()
    if save_checkpoint(final_checkpoint_data):
        print(f"[CHECKPOINT] Final checkpoint saved: {state.pages_processed} total pages processed")
    state.total_results_crawled = len(state.all_results)
    print(f"[MEMORY] Clearing {state.total_results_crawled} result objects from memory...")
    state.all_results = None
    state.crawled_urls_with_depth = None
    state.additional_urls_with_depth = None
    state.crawled_urls_with_depth_merged = None
    state.additional_urls_with_depth_merged = None
    import gc
    gc.collect()
    _clear_soup_cache()
    print(f"[MEMORY] Memory cleanup complete - freed {state.total_results_crawled} result objects")


def _save_error_log_and_exit_checkpoint(state):
    """Save the error log to disk and optionally save a checkpoint on exit/interrupt."""
    import json as json_module
    try:
        timeout_errors = [e for e in state.all_errors if e.get('is_timeout', False)]
        other_errors = [e for e in state.all_errors if not e.get('is_timeout', False)]
        crawled_count = (
            state.total_results_crawled
            if state.total_results_crawled > 0
            else (len(state.all_results) if state.all_results else 0)
        )
        error_log = {
            'timestamp': datetime.now().isoformat(),
            'total_errors': len(state.all_errors),
            'total_successful': len(state.all_successful_urls),
            'total_crawled': crawled_count,
            'timeout_errors': len(timeout_errors),
            'other_errors': len(other_errors),
            'timeout_urls': [
                {'url': e.get('url'), 'error': e.get('error'), 'timestamp': e.get('timestamp')}
                for e in timeout_errors
            ],
            'errors': state.all_errors,
        }
        ERROR_LOG_FILE.write_text(json_module.dumps(error_log, indent=2), encoding="utf-8")
        if state.all_errors:
            print(f"\n[ERROR LOG] Saved {len(state.all_errors)} errors to {ERROR_LOG_FILE}")
    except Exception as log_error:
        print(f"\n[WARNING] Failed to save error log: {log_error}")

    # Save checkpoint on exit (interrupt/time limit) so a new job can resume.
    # Skip if memory cleanup already ran (fields are None) — final checkpoint was
    # already saved in the main flow; re-saving would overwrite valid data with None.
    try:
        if state.crawled_urls_with_depth is not None or state.additional_urls_with_depth is not None:
            checkpoint_data = state.to_checkpoint()
            if save_checkpoint(checkpoint_data):
                print(f"[CHECKPOINT] Checkpoint saved on exit (interrupt/time limit)")
        else:
            print(f"[CHECKPOINT] Skipped — memory cleanup already ran; final checkpoint was saved earlier")
    except Exception as cp_error:
        print(f"\n[WARNING] Failed to save checkpoint on exit: {cp_error}")


def _process_result_worker(work_item):
    """Module-level worker for ProcessPoolExecutor (Issue 3 fix).

    Runs the CPU-heavy per-result pipeline in a separate process:
    markdown extraction, table/image extraction, cleanup, file write.

    Receives a plain dict (picklable) with all needed data.
    Returns a lightweight result dict for the main process to merge into state.
    """
    import types as _types
    from pathlib import Path as _Path
    from datetime import datetime as _dt
    import traceback as _tb

    try:
        # Reconstruct result proxy from plain dict
        if work_item['markdown_is_object']:
            md_proxy = _types.SimpleNamespace(
                fit_markdown=work_item['markdown_fit'],
                raw_markdown=work_item['markdown_raw'],
            )
        else:
            md_proxy = work_item['markdown_str']

        result = _types.SimpleNamespace(
            html=work_item['html'],
            url=work_item['url'],
            markdown=md_proxy,
            metadata=work_item['metadata'],
            redirected_url=work_item['redirected_url'],
            success=work_item['success'],
            tables=work_item['tables'],
            media=work_item['media'],
            cleaned_html=work_item['cleaned_html'],
            error_message=work_item['error_message'],
        )

        result_is_pdf = work_item['result_is_pdf']
        depth_str = work_item['depth_str']
        final_url = work_item['final_url']
        original_url = work_item['original_url']
        url_entry = work_item['url_entry']
        filename = _Path(work_item['filename'])
        bs_available = work_item['bs_available']
        pdf_support = work_item['pdf_support']

        # --- Heavy pipeline ---
        markdown_content = ""
        tables_markdown = ""
        if hasattr(result, "markdown") or (hasattr(result, "html") and result.html):
            markdown_content = _resolve_markdown_content(result, result_is_pdf)

        # Inject links into markdown tables
        if markdown_content and not result_is_pdf and result.html and bs_available:
            try:
                markdown_content = inject_links_into_markdown_tables(markdown_content, result.html)
            except Exception:
                pass

        # Check for crawl errors
        if hasattr(result, 'success') and not result.success:
            error_msg = getattr(result, 'error_message', 'Unknown error')
            return {
                'saved': False, 'is_error': True, 'url': result.url,
                'error': {'url': result.url, 'error': error_msg,
                          'timestamp': _dt.now().isoformat()},
            }

        # Extract tables and images
        tables_markdown, image_refs_markdown = _content_extraction.extract_tables_and_images(
            result, result_is_pdf, pdf_support
        )

        # Build content_to_save
        source_url_for_header = None
        try:
            if final_url and isinstance(final_url, str) and final_url.strip():
                source_url_for_header = final_url.strip()
        except Exception:
            pass
        if not source_url_for_header:
            if result.url:
                url_val = result.url
                if isinstance(url_val, str) and url_val.strip():
                    source_url_for_header = url_val.strip()
                else:
                    source_url_for_header = str(url_val) if url_val else "Unknown URL"
            else:
                source_url_for_header = "Unknown URL"
        if not source_url_for_header or not source_url_for_header.strip():
            source_url_for_header = "Unknown URL"

        url_header = f"# Source URL\n\n{source_url_for_header}\n\n"

        if markdown_content or tables_markdown or image_refs_markdown:
            if markdown_content:
                markdown_content = _markdown_cleanup.clean_raw_markdown(markdown_content, result.url)

        content_to_save = url_header
        text_only_content = ""

        if tables_markdown:
            if markdown_content:
                text_only_content = _markdown_cleanup.deduplicate_markdown_against_tables(
                    markdown_content, tables_markdown,
                    result.url, result.html
                )
            content_to_save += tables_markdown
            if text_only_content:
                content_to_save += "\n\n" + text_only_content
        else:
            if markdown_content:
                content_to_save += markdown_content

        if image_refs_markdown:
            content_to_save += image_refs_markdown

        # External links
        if not result_is_pdf and result.html:
            external_links_markdown = extract_external_links(result.html, result.url)
            if external_links_markdown:
                content_to_save += external_links_markdown

        # Fill empty Name columns and post-process
        content_to_save = _markdown_cleanup.fill_empty_name_columns(content_to_save)
        content_to_save = _markdown_cleanup.post_process_markdown(content_to_save, tables_markdown)

        # Skip empty pages
        if _markdown_cleanup.is_empty_page(content_to_save):
            return {'saved': False, 'is_empty': True,
                    'url': final_url or original_url}

        # Write file
        filename.parent.mkdir(exist_ok=True)
        filename.write_text(content_to_save, encoding="utf-8")

        # Collect return data
        num_tables = len(result.tables) if result.tables else 0
        file_type = "PDF" if result_is_pdf else "HTML"

        pdf_info = []
        if result_is_pdf and pdf_support and result.metadata:
            if result.metadata.get('title'):
                pdf_info.append(f"Title: {result.metadata.get('title')}")
            if result.metadata.get('author'):
                pdf_info.append(f"Author: {result.metadata.get('author')}")

        metadata = _checkpoint.extract_result_metadata(result)

        return {
            'saved': True,
            'url': result.url,
            'filename': str(filename),
            'depth_str': depth_str,
            'url_entry': url_entry,
            'metadata': metadata,
            'file_type': file_type,
            'num_tables': num_tables,
            'pdf_info': pdf_info,
            'markdown_len': len(markdown_content) if markdown_content else 0,
            'final_url': final_url,
            'original_url': original_url,
            'result_is_pdf': result_is_pdf,
        }
    except Exception as e:
        return {
            'saved': False, 'is_error': True,
            'url': work_item.get('url', 'Unknown URL'),
            'error': {'url': work_item.get('url', 'Unknown URL'),
                      'error': f'Exception: {str(e)}',
                      'traceback': _tb.format_exc(),
                      'timestamp': _dt.now().isoformat()},
        }


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
    import time
    start_time = time.time()  # Track start time for elapsed time calculation
    url_metadata_store = {}   # url → {meta_last_modified, last_crawled_at, depth, filename}
    _dt_now = datetime.now().isoformat()

    # ========================================================================
    # DOMAIN-SPECIFIC FILTERING CONFIGURATION
    # ========================================================================
    # Strategy: Apply .inactive CSS selectors ONLY to domains where "inactive" 
    # means "disabled/non-clickable" (semantic meaning). Skip filtering for 
    # domains where "inactive" is part of structural class naming.
    #
    # Verified behavior:
    # - particle-physics.desy.de: .inactive = truly disabled items (filter ✓)
    # - cms.desy.de: .inactive = structural class (skip ✗)
    # - www.desy.de: mostly safe (skip ✗ - conservative approach)
    
    # PHASE 2 FIX: Removed DOMAINS_REQUIRING_INACTIVE_FILTERING dict
    # .inactive selectors now apply universally to all domains
    # This simplifies code and works for 200k+ diverse URLs without domain-specific tuning
    
    # ========================================================================
    # STEP 1: Initialize Error Tracking and Load Checkpoint
    # ========================================================================
    # PHASE 1 FIX: Stream results instead of accumulating in memory
    # These variables track metadata only - results are processed immediately
    # and saved to disk, then discarded to prevent memory exhaustion.
    
    # Load checkpoint if resuming from previous run
    checkpoint = load_checkpoint()
    if checkpoint:
        print(f"[CHECKPOINT] Resuming from checkpoint with {checkpoint.get('pages_processed', 0)} pages already processed")
    
    state = _checkpoint.CrawlState.from_checkpoint(checkpoint)
    
    # Clear redirect-resolution cache so we re-check redirects each run
    global _redirect_resolution_cache
    _redirect_resolution_cache.clear()
    
    # ========================================================================
    # STEPS 2-4: Build browser config, URL filters, crawl strategy, pipeline
    # ========================================================================
    setup = _build_crawl_setup()
    browser_config = setup.browser_config
    deep_crawl_strategy = setup.deep_crawl_strategy
    exclusion_patterns = setup.exclusion_patterns
    # Perf #9: compile all exclusion patterns into a single alternation regex
    # re.IGNORECASE ensures uppercase extensions (.PNG, .PDF, .JPG …) are also blocked.
    _exclusion_re = re.compile('|'.join(exclusion_patterns), re.IGNORECASE) if exclusion_patterns else None
    excluded_selector_str = setup.excluded_selector_str
    INACTIVE_SELECTORS = setup.inactive_selectors
    markdown_generator = setup.markdown_generator
    table_extraction_strategy = setup.table_extraction_strategy
    dedup_enabled = setup.dedup_enabled

    # Reset content dedup tracking for this run
    state.seen_content_hashes = {}
    state.duplicate_count = 0

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
                
                # ROOT_URLs are always at depth 0 (seed URLs)
                current_depth = 0
                
                # ====================================================================
                # PHASE 1 FIX: Skip seed URLs already in checkpoint (with depth check)
                # ====================================================================
                # Check if this seed URL was already crawled in a previous run
                normalized_seed = _normalize_url(root_url) or root_url
                checkpoint_seen_urls = checkpoint.get('seen_final_urls', set())
                checkpoint_seed_urls = checkpoint.get('seed_urls_processed', set())
                checkpoint_max_depth = checkpoint.get('max_depth_crawled', 0)
                
                # Determine if we should skip this seed URL
                should_skip = False
                skip_reason = ""
                _resuming_deeper = False  # True when resuming with higher MAX_DEPTH
                
                if normalized_seed in checkpoint_seed_urls:
                    # Seed URL was processed before
                    if MAX_DEPTH <= checkpoint_max_depth:
                        # Same or lower depth - skip entirely
                        should_skip = True
                        skip_reason = f"already crawled at depth {checkpoint_max_depth}, current MAX_DEPTH={MAX_DEPTH}"
                    else:
                        # Higher depth requested - skip BFS re-crawl, use single-page + frontier fetch
                        should_skip = False
                        _resuming_deeper = True
                        print(f"[CHECKPOINT] Seed URL was crawled at depth {checkpoint_max_depth}, but now MAX_DEPTH={MAX_DEPTH} - will fetch frontier pages only (no BFS re-crawl)")
                
                if should_skip:
                    print(f"[CHECKPOINT SKIP] Seed URL {skip_reason}: {root_url}")
                    continue  # Skip to next seed URL
                
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
                
                # ====================================================================
                # PHASE 2 FIX: Apply .inactive selectors universally to all URLs
                # ====================================================================
                # No longer checking domain whitelist - .inactive filtering applied globally
                # This ensures consistent behavior across all 200k+ URLs
                final_excluded_selector_str = excluded_selector_str + ', ' + ', '.join(INACTIVE_SELECTORS)
                print(f"[CONFIG] Using .inactive filtering for {root_url}")
                print(f"[DEBUG] .inactive filtering ENABLED: Adding {len(INACTIVE_SELECTORS)} additional selectors")
                print(f"[DEBUG] Final CSS selector string length: {len(final_excluded_selector_str)} characters")
                print(f"[DEBUG] Sample selectors applied: .inactive, a.inactive, li[class*='inactive'], [class*='ZMSDocument'][class*='inactive']")
                
                # Create config for this URL
                # CRITICAL FIX: excluded_tags removes nav/footer/header BEFORE link extraction
                # This causes BFSDeepCrawlStrategy to miss links in those sections
                # Solution: Use minimal excluded_tags for link extraction (only script/style/noscript)
                # Keep full excluded_tags for markdown generation (applied later)
                # Note: crawl4ai may apply excluded_tags during link extraction, so we minimize it here
                link_extraction_excluded_tags = ['script', 'style', 'noscript'] if not is_pdf else None  # Minimal filtering for link extraction
                
                _main_crawl_config = CrawlerRunConfig(
                    # deep_crawl_strategy: Tells crawler to follow links using our strategy
                    # CHECKPOINT FIX: On resume with higher depth, disable BFS to avoid
                    # re-fetching all previously-crawled pages. The iterative loop (Step 7)
                    # will discover new URLs from frontier pages instead.
                    deep_crawl_strategy=None if _resuming_deeper else deep_crawl_strategy,
                    
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
                    # Uses final_excluded_selector_str which includes .inactive selectors only if needed
                    excluded_selector=final_excluded_selector_str if not is_pdf else None,
                    
                    # word_count_threshold: Filter out short text blocks (navigation items, labels)
                    # Removes text blocks with fewer than this many words
                    # Helps eliminate navigation menus, short labels, and UI elements
                    word_count_threshold=5 if not is_pdf else None,
                    
                    # remove_forms: Remove all form elements (cookie consent, search forms)
                    # Cookie consent forms are common noise in web scraping
                    remove_forms=True if not is_pdf else None,

                    # Force language/locale for consistent UI strings
                    locale=FORCE_LOCALE,
                    
                    # PHASE 3 FIX: Enable Crawl4AI caching for faster resumption
                    # cache_mode='read_write' enables local caching of crawl results
                    # Benefits: 40% faster resumption, automatic deduplication, reduced network overhead
                    cache_mode=CRAWL_CACHE_MODE,
                    
                    # verbose: Print progress information while crawling
                    verbose=False,  # Disable verbose mode - reduces log by ~33%

                    # Per-page delay to space requests within arun_many (WebOffice-friendly)
                    delay_before_return_html=PAGE_DELAY_MS,
                )
                
                # Update crawler's strategy for this URL (if PDF)
                if crawler_strategy:
                    crawler.crawler_strategy = crawler_strategy
                
                # This is where the magic happens:
                # - Crawler fetches the root page
                # - Extracts all links from the page
                # - Follows those links (up to max_depth)
                # - Returns a list of all crawled pages
                # 
                # CRITICAL FIX: Use original URL (with www.) for crawling to avoid 404 redirects
                # The server at desy.de (without www) has bot detection that redirects specific URLs to 404
                # The server at www.desy.de (with www) works correctly
                # 
                # Note: Normalization is still used for domain matching and deduplication in post-processing,
                # but we must use the original URL for the actual crawl to avoid triggering bot detection
                crawl_url = root_url  # Use original URL, don't normalize before crawling
                
                # Strict URL validation before crawl attempt
                is_valid, skip_reason = _is_valid_crawl_url(crawl_url)
                if not is_valid:
                    # Log skip reason once per type (tracked globally)
                    if not hasattr(crawl_site, '_logged_skip_reasons'):
                        crawl_site._logged_skip_reasons = set()
                    if skip_reason not in crawl_site._logged_skip_reasons:
                        example_url = crawl_url if crawl_url else "N/A"
                        reason_map = {
                            "empty_or_not_string": "Empty or non-string URL",
                            "protocol_mailto": "mailto: protocol",
                            "protocol_tel": "tel: protocol",
                            "protocol_javascript": "javascript: protocol",
                            "protocol_data": "data: protocol",
                            "invalid_scheme": "Non-HTTPS scheme",
                            "fragment_only": "Fragment-only URL (#...)",
                            "relative_url": "Relative URL",
                            "missing_netloc": "Missing netloc"
                        }
                        reason_msg = reason_map.get(skip_reason, skip_reason)
                        print(f"[SKIP] Skipping invalid URL (reason: {reason_msg}): {example_url}")
                        crawl_site._logged_skip_reasons.add(skip_reason)
                    continue
                
                # GROUP 1 FILTER: Check if URL is a login/auth/admin page before crawling
                if should_skip_login_auth_url(crawl_url):
                    print(f'[GROUP 1 FILTER] Skipping login/auth/admin URL: {crawl_url}')
                    state.group1_skipped_count += 1
                    state.duplicate_urls.add(crawl_url)
                    state.crawled_urls_with_depth[_normalize_url(crawl_url) or crawl_url] = current_depth
                    continue
                
                # GROUP 2 FILTER: Check if URL is an error page before crawling
                if should_skip_error_url(crawl_url):
                    print(f'[GROUP 2 FILTER] Skipping error/maintenance URL: {crawl_url}')
                    state.group2_skipped_count += 1
                    state.duplicate_urls.add(crawl_url)
                    state.crawled_urls_with_depth[_normalize_url(crawl_url) or crawl_url] = current_depth
                    continue
                
                # GROUP 4 FILTER: Check if URL is a duplicate due to query parameter variations
                # This checks BEFORE crawling to prevent wasted bandwidth (unlike ContentHash post-crawl)
                # FIX 6.5.1 (Run 18 regression): Exempt seed URLs from GROUP 4 filter.
                # Seed URLs must ALWAYS get full BFS traversal, even if they were seen as
                # navigation links during an earlier seed's crawl. Without this exemption,
                # the English homepage (index_eng.html) was skipped because it was discovered
                # as a nav link from index_ger.html and added to state.seen_normalized_urls.
                is_seed_url = crawl_url in ROOT_URLS or any(_normalize_url(crawl_url) == _normalize_url(seed) for seed in ROOT_URLS)
                if not is_seed_url and should_skip_query_param_duplicate(crawl_url, state.seen_normalized_urls):
                    print(f'[GROUP 4 FILTER] Skipping query-param duplicate URL: {crawl_url}')
                    state.group4_skipped_count += 1
                    state.duplicate_urls.add(crawl_url)
                    state.crawled_urls_with_depth[_normalize_url(crawl_url) or crawl_url] = current_depth
                    continue
                
                # First, crawl the page to get initial results
                # ERROR LOGGING: Wrap in try-except to catch timeout and other errors
                try:
                    _bfs_t0 = time.time()
                    results = await crawler.arun(crawl_url, config=_main_crawl_config)
                    _bfs_elapsed = time.time() - _bfs_t0
                    
                    # Track progress (replaces verbose output)
                    total_urls_crawled = len(results) if isinstance(results, list) else (1 if results else 0)
                    _per_page = _bfs_elapsed / total_urls_crawled if total_urls_crawled > 0 else _bfs_elapsed
                    print(f"[PROGRESS] Crawled {total_urls_crawled} URL(s) from {root_url} "
                          f"in {_bfs_elapsed:.1f}s ({_per_page:.2f}s/page)")
                    
                except Exception as e:
                    # Log timeout and other errors for future retries
                    error_msg = str(e)
                    is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower() or 'TimeoutError' in str(type(e).__name__)
                    
                    error_entry = {
                        'url': crawl_url,  # Use original crawl URL, not normalized
                        'error': error_msg,
                        'error_type': type(e).__name__,
                        'is_timeout': is_timeout,
                        'timestamp': datetime.now().isoformat(),
                        'note': 'Timeout errors can be retried with PAGE_TIMEOUT_EXTENDED in future runs'
                    }
                    state.all_errors.append(error_entry)
                    
                    if is_timeout:
                        print(f"[TIMEOUT] {crawl_url}")  # Full details in state.all_errors JSON
                    else:
                        print(f"[ERROR] {crawl_url}: {error_msg[:50]}")  # Shorter message
                    
                    # Continue with next URL - don't let one failure stop the entire crawl
                    continue
                
                first_result = results[0] if results and len(results) > 0 else None
                
                # FIX: Extract links from full HTML (including nav/footer/header) and crawl them
                # crawl4ai's excluded_tags filters links in nav/footer/header, so we manually extract them
                # This ensures links like "desy.de/desy_in_leichter_sprache/index_ger.html" are crawled
                # CRITICAL: Only extract additional URLs if MAX_DEPTH > 0 (depth 0 = seed pages only)
                additional_urls_to_crawl = []

                if MAX_DEPTH > 0 and first_result and hasattr(first_result, 'html') and first_result.html and BEAUTIFULSOUP_AVAILABLE:
                    try:
                        from urllib.parse import urljoin, urlparse
                        soup = BeautifulSoup(first_result.html, 'lxml')
                        base_url = first_result.url if hasattr(first_result, 'url') and first_result.url else crawl_url
                        base_domain = _normalize_domain(urlparse(base_url).netloc)
                        
                        # Get URLs already crawled in this batch (Section 4.3: fix undefined result_urls)
                        result_urls = []
                        for r in (results if isinstance(results, list) else [results]):
                            if r:
                                if getattr(r, 'url', None):
                                    result_urls.append(r.url)
                                if getattr(r, 'redirected_url', None):
                                    result_urls.append(r.redirected_url)
                        seen_urls = set()
                        for url in result_urls:
                            if url:
                                normalized_seen = _normalize_url(url)
                                seen_urls.add(normalized_seen)
                        
                        # Extract ALL links from HTML (including nav/footer/header)
                        all_links = soup.find_all('a', href=True)
                        _redirect_candidates = []
                        
                        for link in all_links:
                            href = link.get('href', '').strip()
                            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                                continue
                            
                            # Resolve relative URLs
                            absolute_url = urljoin(base_url, href)
                            
                            # Strict URL validation before adding to crawl queue
                            is_valid, skip_reason = _is_valid_crawl_url(absolute_url)
                            if not is_valid:
                                # Log skip reason once per type (tracked globally)
                                if not hasattr(crawl_site, '_logged_skip_reasons'):
                                    crawl_site._logged_skip_reasons = set()
                                if skip_reason not in crawl_site._logged_skip_reasons:
                                    reason_map = {
                                        "empty_or_not_string": "Empty or non-string URL",
                                        "protocol_mailto": "mailto: protocol",
                                        "protocol_tel": "tel: protocol",
                                        "protocol_javascript": "javascript: protocol",
                                        "protocol_data": "data: protocol",
                                        "invalid_scheme": "Non-HTTPS scheme",
                                        "fragment_only": "Fragment-only URL (#...)",
                                        "relative_url": "Relative URL",
                                        "missing_netloc": "Missing netloc"
                                    }
                                    reason_msg = reason_map.get(skip_reason, skip_reason)
                                    print(f"[SKIP] Skipping invalid URL (reason: {reason_msg}): {absolute_url}")
                                    crawl_site._logged_skip_reasons.add(skip_reason)
                                continue
                            
                            parsed = urlparse(absolute_url)
                            
                            # Only include links inside the configured crawl scope.
                            link_domain = _normalize_domain(parsed.netloc)
                            if link_domain == base_domain and _is_allowed_crawl_url(absolute_url):
                                # Never queue links to excluded domains (fater.desy.de, bib-pubdb1.desy.de)
                                if link_domain in EXCLUDED_DOMAINS:
                                    continue
                                # CRITICAL FIX: Keep original URL for crawling (don't normalize)
                                normalized_link = _normalize_url(absolute_url)
                                if normalized_link not in seen_urls:
                                    # GROUP 4 FILTER: Skip UI-param duplicates (@@siteview?printversion=1, etc.)
                                    if should_skip_query_param_duplicate(absolute_url, state.seen_normalized_urls):
                                        continue
                                    # FIX (Issues 6, 7, 8): Apply exclusion_patterns to additional_urls path.
                                    if _exclusion_re and _exclusion_re.search(absolute_url):
                                        continue
                                    _redirect_candidates.append((absolute_url, normalized_link))
                                    seen_urls.add(normalized_link)  # reserve early to avoid duplicates
                        
                        # Batch-resolve redirect checks for all link candidates (asyncio.gather)
                        if CHECK_REDIRECTS_TO_EXCLUDED and _redirect_candidates:
                            _hosts = await asyncio.gather(
                                *[_resolve_redirect_final_host(u) for u, _ in _redirect_candidates],
                                return_exceptions=True
                            )
                            for (_cand_url, _cand_norm), _host in zip(_redirect_candidates, _hosts):
                                if isinstance(_host, Exception) or _host is None or _host not in EXCLUDED_DOMAINS:
                                    additional_urls_to_crawl.append(_cand_url)
                        else:
                            additional_urls_to_crawl.extend(u for u, _ in _redirect_candidates)
                    except Exception as e:
                        # HTML link extraction failed — log but continue
                        pass

                # Crawl additional URLs extracted from HTML.
                if additional_urls_to_crawl:
                    urls_to_crawl_batch = additional_urls_to_crawl[:500]
                    
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
                        cache_mode=CRAWL_CACHE_MODE,
                        locale=FORCE_LOCALE,
                        verbose=True,
                        delay_before_return_html=PAGE_DELAY_MS,
                        # NOTE: No deep_crawl_strategy - this ensures single page crawl only
                    )
                    
                    # Pre-filter login/auth URLs before batch crawling
                    filtered_urls_batch = []
                    for additional_url in urls_to_crawl_batch:
                        if should_skip_login_auth_url(additional_url):
                            print(f'[GROUP 1 FILTER] Skipping login/auth URL (additional): {additional_url}')
                            continue
                        filtered_urls_batch.append(additional_url)
                    
                    if filtered_urls_batch:
                        print(f"[INFO] Crawling {len(filtered_urls_batch)} single-page URLs with arun_many (parallel)")
                        try:
                            _dispatcher = MemoryAdaptiveDispatcher(
                                max_session_permit=50,
                                rate_limiter=RateLimiter(
                                    #base_delay=(0.2, 2.0), max_delay=16.0, base*2^(retry_number)
                                    base_delay=(0.05, 0.5), max_delay=60.0,
                                    max_retries=3, rate_limit_codes=[429]
                                )
                            )
                            batch_results = await crawler.arun_many(filtered_urls_batch, config=single_page_config, dispatcher=_dispatcher)
                            
                            def _passes_exclusion(url):
                                return not (_exclusion_re and _exclusion_re.search(url))
                            for additional_result in batch_results:
                                if isinstance(additional_result, list):
                                    state.all_results.extend(
                                        r for r in additional_result
                                        if r and getattr(r, 'url', None) and _is_allowed_crawl_url(r.url) and _passes_exclusion(r.url)
                                    )
                                elif additional_result and getattr(additional_result, 'url', None) and _is_allowed_crawl_url(additional_result.url) and _passes_exclusion(additional_result.url):
                                    state.all_results.append(additional_result)
                        except Exception as e:
                            error_msg = str(e)
                            is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower() or 'TimeoutError' in str(type(e).__name__)
                            
                            error_entry = {
                                'url': 'batch_crawl_additional_urls',
                                'error': error_msg,
                                'error_type': type(e).__name__,
                                'is_timeout': is_timeout,
                                'timestamp': datetime.now().isoformat(),
                                'note': 'Batch crawl failed - some URLs may not have been processed'
                            }
                            state.all_errors.append(error_entry)
                
                # Accumulate initial results — filter out pages outside the allowed scope.
                # BFS with include_external=False may still return pages from other DESY
                # subdomains.  Filtering here prevents those pages from being queued for
                # link extraction in STEP 7 and from being written to disk in STEP 8.
                # FIX (Root Cause 2, part 5): Also drop results matching exclusion_patterns.
                def _passes_exclusion(url):
                    return not (_exclusion_re and _exclusion_re.search(url))
                if isinstance(results, list):
                    state.all_results.extend(
                        r for r in results
                        if r and getattr(r, 'url', None) and _is_allowed_crawl_url(r.url) and _passes_exclusion(r.url)
                    )
                else:
                    if results and getattr(results, 'url', None) and _is_allowed_crawl_url(results.url) and _passes_exclusion(results.url):
                        state.all_results.append(results)
                
                # PHASE 1 FIX: Track that this seed URL was processed
                state.seed_urls_processed.add(normalized_seed)
                # Update state.max_depth_crawled to track the deepest level we've crawled
                state.max_depth_crawled = max(state.max_depth_crawled, MAX_DEPTH)
                print(f"[CHECKPOINT] Marked seed URL as processed: {root_url} (depth: {MAX_DEPTH})")
            
            # ====================================================================
            # STEP 7: Extract links from ALL results (not just seed URL)
            # ====================================================================
            # CRITICAL FIX: Manual link extraction should run for ALL pages, not just seed URL
            # crawl4ai's excluded_tags filters links in nav/footer/header, so we manually extract them
            # from ALL crawled pages to ensure no links are missed
            # NOTE: This step is SKIPPED when MAX_DEPTH == 0 (only seed pages should be crawled)
            
            # Collect all additional URLs to crawl from all results
            # Track URL -> source_depth mapping to assign correct depths
            all_additional_urls = {}  # {normalized_url: source_depth}
            seen_crawled_urls = set()
            # CHECKPOINT FIX: Seed seen_crawled_urls from checkpoint so the iterative
            # loop skips URLs already processed in previous runs.
            if state.seen_final_urls:
                seen_crawled_urls.update(state.seen_final_urls)
                print(f"[CHECKPOINT] Seeded seen_crawled_urls with {len(state.seen_final_urls)} URLs from checkpoint")
            # Track depth mapping for additional URLs (will be populated when crawling them)
            state.additional_urls_with_depth = {}  # {normalized_url: depth} - populated during crawling

            # First, collect all URLs that were already crawled AND their depths
            # This prevents us from reassigning depth to URLs that were already crawled by BFSDeepCrawlStrategy
            # CHECKPOINT FIX: Preserve checkpoint's crawled_urls_with_depth instead of resetting.
            # On resume, state.all_results is sparse (only current run), so resetting would
            # lose depth information from previous runs.
            _checkpoint_depths = dict(state.crawled_urls_with_depth)  # preserve checkpoint data
            state.crawled_urls_with_depth = {}  # reset for fresh population from current results
            for result in state.all_results:
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
                        normalized_source = _normalize_url(source_url)
                        for root_url in ROOT_URLS:
                            normalized_seed = _normalize_url(root_url) or root_url
                            if normalized_source == normalized_seed:
                                result_depth = 0
                                break
                    
                    # If depth is 0 and not seed, default to 1
                    if result_depth == 0 and source_url:
                        is_seed = False
                        normalized_source = _normalize_url(source_url)
                        for root_url in ROOT_URLS:
                            normalized_seed = _normalize_url(root_url) or root_url
                            if normalized_source == normalized_seed:
                                is_seed = True
                                break
                        if not is_seed:
                            result_depth = 1
                    
                    # Store URL and depth (keep shallowest depth to avoid reclassification)
                    if hasattr(result, 'url') and result.url:
                        normalized = _normalize_url(result.url)
                        seen_crawled_urls.add(normalized)
                        existing_depth = state.crawled_urls_with_depth.get(normalized)
                        state.crawled_urls_with_depth[normalized] = result_depth if existing_depth is None else min(existing_depth, result_depth)
                    if hasattr(result, 'redirected_url') and result.redirected_url:
                        normalized = _normalize_url(result.redirected_url)
                        seen_crawled_urls.add(normalized)
                        existing_depth = state.crawled_urls_with_depth.get(normalized)
                        state.crawled_urls_with_depth[normalized] = result_depth if existing_depth is None else min(existing_depth, result_depth)
            
            # CHECKPOINT FIX: Merge back checkpoint depth data (for URLs not in current results)
            for _cp_url, _cp_depth in _checkpoint_depths.items():
                if _cp_url not in state.crawled_urls_with_depth:
                    state.crawled_urls_with_depth[_cp_url] = _cp_depth
                    seen_crawled_urls.add(_cp_url)
                else:
                    state.crawled_urls_with_depth[_cp_url] = min(state.crawled_urls_with_depth[_cp_url], _cp_depth)
            del _checkpoint_depths  # free memory

            # ================================================================

            # ================================================================
            # STEP 8 shared state — initialised early so results can be
            # processed incrementally during STEP 7 (Option B memory fix).
            # ================================================================
            results_metadata = []
            
            # Track seed URLs (normalized) to ensure they get depth 0
            seed_urls_normalized = set()
            for root_url in ROOT_URLS:
                normalized_seed = _normalize_url(root_url) or root_url
                seed_urls_normalized.add(normalized_seed)
            
            # PHASE 1 FIX: Load state.seen_final_urls from checkpoint for crash recovery
            # If resuming, skip URLs already processed in previous run
            state.seen_final_urls = checkpoint.get('seen_final_urls', set())
            if state.seen_final_urls:
                print(f"[CHECKPOINT] Loaded {len(state.seen_final_urls)} already-processed URLs from checkpoint")
            
            # state.additional_urls_with_depth is defined in the link extraction section above
            # It maps normalized URLs to their assigned depths
            # Build merged dict using minimum depth per URL across checkpoint and current run.
            # This preserves the shallowest known path from seed when duplicates are rediscovered.
            state.additional_urls_with_depth_merged = dict(checkpoint.get('additional_urls_with_depth', {}))
            for url_key, depth_value in state.additional_urls_with_depth.items():
                existing_depth = state.additional_urls_with_depth_merged.get(url_key)
                state.additional_urls_with_depth_merged[url_key] = depth_value if existing_depth is None else min(existing_depth, depth_value)
            # Section 4.1/4.2: Build state.crawled_urls_with_depth_merged using minimum depth per URL across checkpoint and current run.
            # This preserves the shallowest known path from seed when the same URL is rediscovered at a deeper level.
            state.crawled_urls_with_depth_merged = dict(checkpoint.get('crawled_urls_with_depth', {}))
            for url_key, depth_value in state.crawled_urls_with_depth.items():
                existing_depth = state.crawled_urls_with_depth_merged.get(url_key)
                state.crawled_urls_with_depth_merged[url_key] = depth_value if existing_depth is None else min(existing_depth, depth_value)
            
            # Track links found vs URLs crawled for analysis
            links_found_vs_crawled = {
                'total_links_found_in_html': 0,
                'total_urls_crawled': 0,
                'links_found_by_page': []
            }
            
            # PHASE 1 FIX: Counter for checkpoint frequency
            results_processed_in_batch = 0

            # FIX B: Varnish 503 retry — detect transient backend-overload errors during
            # the main pass and re-crawl them once after a 10-second backoff.
            # "Varnish cache server" never appears in real DESY page content, so these
            # markers are safe to use as a content-level filter.
            _varnish_503_markers = ('Varnish cache server', 'Backend fetch failed', 'Guru Meditation')
            state.varnish_503_retry_queue = []  # URLs that returned a 503 Varnish page
            _process_cursor = 0  # tracks how far we have processed

            def _rebuild_merged_depth_maps():
                """Rebuild merged depth maps from checkpoint + current run."""
                state.crawled_urls_with_depth_merged = dict(checkpoint.get('crawled_urls_with_depth', {}))
                for _url_k, _dep_v in (state.crawled_urls_with_depth or {}).items():
                    _ex = state.crawled_urls_with_depth_merged.get(_url_k)
                    state.crawled_urls_with_depth_merged[_url_k] = _dep_v if _ex is None else min(_ex, _dep_v)
                state.additional_urls_with_depth_merged = dict(checkpoint.get('additional_urls_with_depth', {}))
                for _url_k, _dep_v in (state.additional_urls_with_depth or {}).items():
                    _ex = state.additional_urls_with_depth_merged.get(_url_k)
                    state.additional_urls_with_depth_merged[_url_k] = _dep_v if _ex is None else min(_ex, _dep_v)

            def _preprocess_one_result(result):
                """Serial pre-pass: filter, dedup, depth-assign. Returns work_item dict or None."""
                # 6-stage content filter: scope, login, binary, login-wall, blank, Varnish 503
                _filter_action, _filter_reason = _content_extraction.filter_result_pre_save(
                    result,
                    scope_filter_fn=_is_allowed_crawl_url,
                    login_filter_fn=should_skip_login_auth_url,
                    varnish_503_markers=_varnish_503_markers,
                )
                if _filter_action == 'skip':
                    if _filter_reason == 'login_wall_content':
                        print(f'[CONTENT FILTER] Skipping login-wall page (short + keywords): {result.url}')
                    elif _filter_reason == 'blank_page':
                        print(f'[CONTENT FILTER] Skipping blank/empty page (<50 chars body): {result.url}')
                    return None
                if _filter_action == 'varnish_503':
                    state.varnish_503_retry_queue.append(result.url)
                    print(f'[503-RETRY] Queueing for retry (Varnish 503): {result.url}')
                    return None

                try:
                    # Extract original/final URLs and normalise them
                    original_url, final_url, is_redirect, normalized_original, normalized_final = (
                        _content_extraction.extract_result_urls(result)
                    )
                    
                    # Skip 404 pages with no meaningful content
                    if _content_extraction.is_404_without_content(result, normalized_final):
                        return None
                    
                    # Skip if we've already seen this final URL (deduplication).
                    # FIX 6b: Use normalize_url_for_dedup so that UI-param variants
                    # (?printversion=1, ?view=workWeek, ?two_columns=1, etc.) are
                    # recognised as duplicates of the already-saved base page.
                    _dedup_key = normalize_url_for_dedup(normalized_final) if normalized_final else normalized_final
                    if _dedup_key and _dedup_key in state.seen_final_urls:
                        return None
                    state.seen_final_urls.add(_dedup_key if _dedup_key else normalized_final)
                    
                    # FEATURE #4: Check for duplicate content (Sara Taheri issue)
                    if dedup_enabled:
                        content_to_check = None
                        if hasattr(result, 'markdown'):
                            if hasattr(result.markdown, 'fit_markdown') and result.markdown.fit_markdown:
                                content_to_check = result.markdown.fit_markdown
                            elif hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
                                content_to_check = result.markdown.raw_markdown
                            elif isinstance(result.markdown, str):
                                content_to_check = result.markdown
                        elif hasattr(result, 'html') and result.html:
                            content_to_check = result.html
                        
                        if content_to_check:
                            is_dup, original_dup_url, content_hash = _content_extraction.is_duplicate_content(content_to_check, state.seen_content_hashes, dedup_enabled, final_url)
                            if is_dup:
                                state.duplicate_count += 1
                                print(f"[DEDUP] Skipping duplicate content from {final_url} (same as {original_dup_url})")
                                return None
                    

                    if normalized_original in seed_urls_normalized or normalized_final in seed_urls_normalized:
                        depth = 0  # Seed URL - always depth 0
                    else:
                        depth = _content_extraction.assign_page_depth(
                            normalized_original,
                            normalized_final,
                            seed_urls_normalized,
                            state.crawled_urls_with_depth_merged,
                            state.additional_urls_with_depth_merged,
                            result,
                            MAX_DEPTH,
                        )
                    
                    depth_str = str(depth)
                    if depth_str not in state.all_urls_by_depth:
                        state.all_urls_by_depth[depth_str] = []
                    
                    url_entry = {
                        'original_url': original_url,
                        'final_url': final_url,
                        'is_redirect': is_redirect
                    }
                    
                    # Track links found in HTML vs URLs crawled
                    _base_url_for_links = final_url if final_url else original_url
                    links_in_html = _content_extraction.count_internal_links(result, _base_url_for_links)
                    
                    links_found_vs_crawled['total_links_found_in_html'] += links_in_html
                    links_found_vs_crawled['total_urls_crawled'] += 1
                    links_found_vs_crawled['links_found_by_page'].append({
                        'url': final_url or original_url,
                        'depth': depth_str,
                        'links_found': links_in_html
                    })
                    
                    # Create a safe filename from the URL
                    url_for_filename = _dedup_key if _dedup_key else (final_url if final_url else original_url)
                    url_safe = url_for_filename.replace("https://", "").replace("http://", "")
                    url_safe = url_safe.replace("/", "_").replace(":", "_")
                    if len(url_safe) > 200:
                        url_safe = url_safe[:200]
                    
                    depth_dir = OUTPUT_DIR / f"depth_{depth_str}"
                    depth_dir.mkdir(exist_ok=True)
                    filename = depth_dir / f"{url_safe}.md"
                    
                    result_is_pdf = is_pdf_url(result.url)
                    
                    # Build picklable work_item dict for the worker
                    markdown_fit = None
                    markdown_raw = None
                    markdown_str_val = None
                    markdown_is_object = False
                    if hasattr(result, 'markdown'):
                        if hasattr(result.markdown, 'fit_markdown'):
                            markdown_fit = result.markdown.fit_markdown
                            markdown_raw = getattr(result.markdown, 'raw_markdown', None)
                            markdown_is_object = True
                        elif isinstance(result.markdown, str):
                            markdown_str_val = result.markdown
                    
                    return {
                        'url': result.url,
                        'html': getattr(result, 'html', None),
                        'markdown_fit': markdown_fit,
                        'markdown_raw': markdown_raw,
                        'markdown_str': markdown_str_val,
                        'markdown_is_object': markdown_is_object,
                        'metadata': getattr(result, 'metadata', None),
                        'redirected_url': getattr(result, 'redirected_url', None),
                        'success': getattr(result, 'success', True),
                        'tables': getattr(result, 'tables', None),
                        'media': getattr(result, 'media', None),
                        'cleaned_html': getattr(result, 'cleaned_html', None),
                        'error_message': getattr(result, 'error_message', None),
                        'result_is_pdf': result_is_pdf,
                        'depth_str': depth_str,
                        'final_url': final_url,
                        'original_url': original_url,
                        'is_redirect': is_redirect,
                        'url_entry': url_entry,
                        'filename': str(filename),
                        'links_in_html': links_in_html,
                        'bs_available': BEAUTIFULSOUP_AVAILABLE,
                        'pdf_support': PDF_SUPPORT_AVAILABLE,
                    }
                except Exception as e:
                    import traceback
                    error_url = result.url if result and getattr(result, 'url', None) else "Unknown URL"
                    state.all_errors.append({
                        'url': error_url,
                        'error': f'Exception: {str(e)}',
                        'traceback': traceback.format_exc(),
                        'timestamp': datetime.now().isoformat()
                    })
                    print(f"[ERROR] {error_url}")
                    print(f"        Exception: {str(e)}")
                    return None

            def _merge_worker_result(wr):
                """Merge a worker result dict back into shared state (serial)."""
                nonlocal results_processed_in_batch
                if wr is None:
                    return

                if wr.get('is_error'):
                    state.all_errors.append(wr['error'])
                    print(f"[ERROR] {wr['error']['url']}")
                    print(f"        Reason: {wr['error'].get('error', '')}")
                    if 'traceback' in wr['error']:
                        print(f"        Traceback:\n{wr['error']['traceback']}")
                    return

                if wr.get('is_empty'):
                    print(f"[SKIP] Empty/minimal content page: {wr['url']}")
                    return

                if not wr.get('saved'):
                    return

                depth_str = wr['depth_str']
                if depth_str not in state.all_urls_by_depth:
                    state.all_urls_by_depth[depth_str] = []
                state.all_urls_by_depth[depth_str].append(wr['url_entry'])

                if wr.get('num_tables', 0) > 0:
                    print(f"[{wr['file_type']}] Extracted {wr['num_tables']} table(s) with links preserved from {wr['url']}")

                if wr.get('result_is_pdf') and wr.get('pdf_info'):
                    print(f"[PDF] Metadata: {', '.join(wr['pdf_info'])}")
                    print(f"[PDF] Extracted {wr.get('markdown_len', 0)} characters from PDF")

                state.all_successful_urls.append(wr['url'])
                file_type = wr.get('file_type', 'HTML')
                print(f"[SAVED] [{file_type}] {wr['url']}")
                print(f"        → {wr['filename']}")

                state.pages_processed += 1
                results_processed_in_batch += 1

                if wr.get('metadata'):
                    results_metadata.append(wr['metadata'])
                    _md = wr['metadata']
                    _url = _md.get('url') or wr.get('url')
                    if _url and _md.get('meta_last_modified'):
                        url_metadata_store[_url] = {
                            'meta_last_modified': _md['meta_last_modified'],
                            'last_crawled_at': _dt_now,
                            'depth': wr.get('depth_str'),
                            'filename': wr.get('filename'),
                        }

                # Periodic checkpoint saving for crash recovery
                if results_processed_in_batch % CHECKPOINT_FREQUENCY == 0:
                    import sys
                    checkpoint_start_time = time.time()
                    sys.stdout.flush()

                    checkpoint_data = state.to_checkpoint()
                    checkpoint_saved = save_checkpoint(checkpoint_data)
                    checkpoint_duration = time.time() - checkpoint_start_time

                    try:
                        elapsed_time = time.time() - start_time
                        rate = state.pages_processed / elapsed_time if elapsed_time > 0 else 0
                        print(f"[SUMMARY] Progress: {state.pages_processed} pages processed, "
                              f"{len(state.all_errors)} errors, {len(state.seen_final_urls)} unique URLs, "
                              f"{rate:.1f} pages/sec, checkpoint: {'saved' if checkpoint_saved else 'failed'}, "
                              f"checkpoint+flush: {checkpoint_duration:.2f}s")
                    except Exception:
                        print(f"[SUMMARY] Progress: {state.pages_processed} pages processed, "
                              f"{len(state.all_errors)} errors, {len(state.seen_final_urls)} unique URLs, "
                              f"checkpoint: {'saved' if checkpoint_saved else 'failed'}, "
                              f"checkpoint+flush: {checkpoint_duration:.2f}s")

                    if checkpoint_saved:
                        print(f"[CHECKPOINT] Saved progress: {state.pages_processed} pages processed")

            def _process_one_result(result):
                """STEP 8 per-result pipeline (serial): filter, dedup, depth-assign, write, cleanup."""
                item = _preprocess_one_result(result)
                if item is None:
                    return
                wr = _process_result_worker(item)
                _merge_worker_result(wr)
                # Clear heavy data to free memory
                for _attr in ('html', 'markdown', 'tables', 'cleaned_html', 'fit_markdown'):
                    if hasattr(result, _attr):
                        setattr(result, _attr, None)

            # Perf #14: create a single ProcessPoolExecutor and reuse it across batches.
            from concurrent.futures import ProcessPoolExecutor as _PPE
            _shared_executor = _PPE(max_workers=min(PARALLEL_WORKERS, 64)) if PARALLEL_WORKERS > 0 else None

            def _process_batch(batch):
                """Process a batch of results, using parallel workers if configured."""
                if not batch:
                    return
                if PARALLEL_WORKERS <= 0 or len(batch) <= 1 or _shared_executor is None:
                    for r in batch:
                        _process_one_result(r)
                    return

                # Phase 1: Serial pre-pass (filter, dedup, depth — touches shared state)
                work_items = []
                for r in batch:
                    item = _preprocess_one_result(r)
                    if item is not None:
                        work_items.append(item)
                if not work_items:
                    return

                # Phase 2: Parallel heavy work (markdown, tables, cleanup, file write)
                n_workers = min(PARALLEL_WORKERS, len(work_items))
                print(f"[PARALLEL] Dispatching {len(work_items)} results to {n_workers} workers")
                try:
                    worker_results = list(_shared_executor.map(_process_result_worker, work_items))
                except Exception as pool_error:
                    print(f"[WARNING] Parallel processing failed ({pool_error}), falling back to serial")
                    worker_results = [_process_result_worker(item) for item in work_items]

                # Phase 3: Serial merge (update state from worker results)
                for wr in worker_results:
                    _merge_worker_result(wr)

                # Clear original results to free memory
                for r in batch:
                    for _attr in ('html', 'markdown', 'tables', 'cleaned_html', 'fit_markdown'):
                        if hasattr(r, _attr):
                            setattr(r, _attr, None)

            # ====================================================================

            # Iterative link extraction + single-page crawling
            # ================================================================
            # Strategy: extract in-scope links from crawled pages, crawl new
            # ones as single pages (no inner BFS), then re-scan the new pages
            # for more links.  Repeat until no new URLs or MAX_DEPTH is reached.
            #
            # This replaces the old inner-BFS approach where each additional URL
            # was crawled with remaining_depth = MAX_DEPTH - assigned_depth.
            # That formula made the expansion budget MAX_DEPTH-dependent, causing
            # different depth-2 counts when MAX_DEPTH changed from 2 to 3.
            #
            # With iterative single-page discovery the same URLs are found at
            # each depth level regardless of MAX_DEPTH → stable per-depth counts.

            if MAX_DEPTH > 0 and BEAUTIFULSOUP_AVAILABLE:
                from urllib.parse import urljoin, urlparse

                # Single-page config — no deep_crawl_strategy so arun_many works
                _additional_cfg = CrawlerRunConfig(
                    page_timeout=PAGE_TIMEOUT,
                    wait_until='networkidle',
                    scraping_strategy=None,
                    table_extraction=table_extraction_strategy if TABLE_EXTRACTION_AVAILABLE else None,
                    markdown_generator=markdown_generator if PRUNING_FILTER_AVAILABLE else None,
                    excluded_tags=['script', 'style', 'noscript'],
                    excluded_selector=excluded_selector_str,
                    word_count_threshold=5,
                    remove_forms=True,
                    cache_mode=CRAWL_CACHE_MODE,
                    locale=FORCE_LOCALE,
                    verbose=True,
                    delay_before_return_html=PAGE_DELAY_MS,
                )

                # ============================================================
                # CHECKPOINT FIX (Change 3): Fetch frontier pages on resume
                # ============================================================
                # When resuming with higher MAX_DEPTH, the iterative loop needs
                # HTML from frontier pages (depth == previous max) to discover
                # new links at depth > previous max.  Without this, the loop
                # only has the seed page and cannot reach deeper pages.
                # crawl4ai cache_mode='read_write' should serve these from
                # cache, so network cost is minimal.
                _cp_max_depth = checkpoint.get('max_depth_crawled', 0)
                if _cp_max_depth > 0 and _cp_max_depth < MAX_DEPTH:
                    _existing_urls = {
                        _normalize_url(getattr(r, 'url', ''))
                        for r in state.all_results if r and getattr(r, 'url', None)
                    }
                    _frontier_urls = [
                        url for url, depth in state.crawled_urls_with_depth.items()
                        if depth == _cp_max_depth and url not in _existing_urls
                    ]
                    if _frontier_urls:
                        print(f"[CHECKPOINT] Fetching {len(_frontier_urls)} frontier pages "
                              f"(depth={_cp_max_depth}) for link extraction")
                        try:
                            _fr_dispatcher = MemoryAdaptiveDispatcher(
                                max_session_permit=50,
                                rate_limiter=RateLimiter(
                                    base_delay=(0.05, 0.5), max_delay=60.0,
                                    max_retries=3, rate_limit_codes=[429]
                                )
                            )
                            _frontier_results = await crawler.arun_many(
                                _frontier_urls, config=_additional_cfg,
                                dispatcher=_fr_dispatcher
                            )
                            _frontier_count = 0
                            for _fr in _frontier_results:
                                _items = _fr if isinstance(_fr, list) else ([_fr] if _fr else [])
                                for r in _items:
                                    if not r or not getattr(r, 'url', None):
                                        continue
                                    if not _is_allowed_crawl_url(r.url):
                                        continue
                                    if _exclusion_re and _exclusion_re.search(r.url):
                                        continue
                                    state.all_results.append(r)
                                    _frontier_count += 1
                                    r_norm = _normalize_url(r.url)
                                    if r_norm:
                                        seen_crawled_urls.add(r_norm)
                            print(f"[CHECKPOINT] Fetched {_frontier_count} frontier pages "
                                  f"for link extraction")
                        except Exception as e:
                            print(f"[WARNING] Frontier page fetch failed: {str(e)[:100]}")
                    del _existing_urls  # free memory

                _scan_start = 0        # index into state.all_results: how far we've scanned
                _pass = 0              # pass counter
                _total_additional = 0  # total additional URLs crawled across all passes

                while True:
                    _pass += 1
                    _new_urls = {}  # {normalized_url: assigned_depth} discovered this pass

                    # --- scan un-processed results for outgoing links ----------
                    for result in state.all_results[_scan_start:]:
                        if not result or not hasattr(result, 'html') or not result.html:
                            continue
                        try:
                            source_url = result.url if hasattr(result, 'url') and result.url else None
                            if not source_url:
                                continue
                            normalized_source = _normalize_url(source_url)

                            # Determine source depth — prefer min-depth from the
                            # depth map (link-graph truth) over metadata.depth
                            # which may reflect a longer BFS path.
                            _map_d = state.crawled_urls_with_depth.get(normalized_source)
                            if _map_d is not None:
                                source_depth = _map_d
                            else:
                                source_depth = 0
                                if hasattr(result, 'metadata') and result.metadata:
                                    source_depth = result.metadata.get('depth', 0)
                                elif hasattr(result, 'depth'):
                                    source_depth = result.depth

                            # Seed URLs are always depth 0
                            is_seed = False
                            for root_url in ROOT_URLS:
                                if normalized_source == (_normalize_url(root_url) or root_url):
                                    source_depth = 0
                                    is_seed = True
                                    break
                            # Non-seed at depth 0 → default to 1
                            if source_depth == 0 and not is_seed:
                                source_depth = 1

                            soup = _get_cached_soup(result)
                            if not soup:
                                continue

                            _redir_pending = []
                            for link in soup.find_all('a', href=True):
                                href = link.get('href', '').strip()
                                if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                                    continue

                                absolute_url = urljoin(source_url, href)
                                parsed = urlparse(absolute_url)
                                link_domain = _normalize_domain(parsed.netloc)

                                if link_domain in EXCLUDED_DOMAINS or not _is_allowed_crawl_url(absolute_url):
                                    continue

                                normalized_link = _normalize_url(absolute_url) or absolute_url.replace('://www.', '://')
                                assigned_depth = source_depth + 1

                                if assigned_depth > MAX_DEPTH:
                                    continue

                                if _exclusion_re and _exclusion_re.search(absolute_url):
                                    continue

                                # Already crawled → just update min depth
                                if normalized_link in seen_crawled_urls:
                                    existing = state.crawled_urls_with_depth.get(normalized_link)
                                    if existing is None or assigned_depth < existing:
                                        state.crawled_urls_with_depth[normalized_link] = assigned_depth
                                    continue

                                # Already queued in a previous pass → update min depth
                                if normalized_link in all_additional_urls:
                                    all_additional_urls[normalized_link] = min(all_additional_urls[normalized_link], assigned_depth)
                                    continue

                                # Defer redirect check to batch resolution below
                                if CHECK_REDIRECTS_TO_EXCLUDED:
                                    _redir_pending.append((absolute_url, normalized_link, assigned_depth))
                                else:
                                    if normalized_link not in _new_urls:
                                        _new_urls[normalized_link] = assigned_depth
                                    else:
                                        _new_urls[normalized_link] = min(_new_urls[normalized_link], assigned_depth)

                            # Batch-resolve redirect checks for all candidates in this result
                            if _redir_pending:
                                _hosts = await asyncio.gather(
                                    *[_resolve_redirect_final_host(u) for u, _, _ in _redir_pending],
                                    return_exceptions=True
                                )
                                for (_purl, _pnorm, _pdepth), _host in zip(_redir_pending, _hosts):
                                    if isinstance(_host, Exception) or _host is None or _host not in EXCLUDED_DOMAINS:
                                        if _pnorm not in _new_urls:
                                            _new_urls[_pnorm] = _pdepth
                                        else:
                                            _new_urls[_pnorm] = min(_new_urls[_pnorm], _pdepth)
                        except Exception:
                            pass

                    _scan_start = len(state.all_results)

                    # Incremental STEP 8: process already-scanned results to
                    # free HTML/markdown memory before the next crawl pass.
                    if _process_cursor < _scan_start:
                        _rebuild_merged_depth_maps()
                        _inc_batch = [r for r in state.all_results[_process_cursor:_scan_start] if r is not None]
                        _inc_batch.sort(key=lambda r: len(getattr(r, 'url', '') or ''))
                        _process_batch(_inc_batch)
                        for _ri in range(_process_cursor, _scan_start):
                            state.all_results[_ri] = None
                        _process_cursor = _scan_start
                        del _inc_batch
                        import gc; gc.collect()
                        _clear_soup_cache()  # Perf #10: free cached BeautifulSoup objects between passes

                    if not _new_urls:
                        break

                    # Log this pass
                    _depth_counts = {}
                    for _, d in _new_urls.items():
                        _depth_counts[d] = _depth_counts.get(d, 0) + 1
                    print(f"[INFO] Pass {_pass}: Found {len(_new_urls)} additional URLs by depth: {_depth_counts}")

                    # Record in global tracker
                    all_additional_urls.update(_new_urls)

                    # --- crawl new URLs as single pages (parallel via arun_many) ---
                    _batch = list(_new_urls.keys())[:10000]
                    print(f"[INFO] Pass {_pass}: Crawling {len(_batch)} URLs with arun_many (parallel)")

                    try:
                        # Custom dispatcher: remove 503 from auto-retry codes so crawl4ai's
                        # built-in retry does NOT re-request 503 pages automatically.
                        # Our retry_varnish_503_pages() handles 503s explicitly instead.
                        _dispatcher = MemoryAdaptiveDispatcher(
                            max_session_permit=50,
                            rate_limiter=RateLimiter(
                                    #base_delay=(0.2, 2.0), max_delay=16.0, base*2^(retry_number)
                                    base_delay=(0.05, 0.5), max_delay=60.0,
                                max_retries=3, rate_limit_codes=[429]
                            )
                        )
                        _batch_results = await crawler.arun_many(_batch, config=_additional_cfg, dispatcher=_dispatcher)
                        if INTER_BATCH_DELAY > 0:
                            await asyncio.sleep(INTER_BATCH_DELAY)
                    except Exception as e:
                        state.all_errors.append({
                            'url': f'batch_pass_{_pass}',
                            'error': str(e),
                            'error_type': type(e).__name__,
                            'is_timeout': 'timeout' in str(e).lower(),
                            'timestamp': datetime.now().isoformat(),
                            'note': f'Batch crawl failed in pass {_pass}'
                        })
                        print(f"[ERROR] Pass {_pass} batch failed: {str(e)[:100]}")
                        break

                    _new_count = 0
                    for _res_item in _batch_results:
                        _items = _res_item if isinstance(_res_item, list) else ([_res_item] if _res_item else [])
                        for r in _items:
                            if not r:
                                continue
                            # Scope filter: skip pages outside allowed prefix
                            if not getattr(r, 'url', None) or not _is_allowed_crawl_url(r.url):
                                continue
                            # FIX (Root Cause 2, part 5): Drop results matching exclusion_patterns
                            if _exclusion_re and _exclusion_re.search(r.url):
                                continue
                            state.all_results.append(r)
                            _new_count += 1

                            # Update depth maps
                            r_url = getattr(r, 'url', None)
                            r_norm = _normalize_url(r_url) if r_url else None
                            if r_norm:
                                seen_crawled_urls.add(r_norm)
                                _d = _new_urls.get(r_norm, all_additional_urls.get(r_norm))
                                if _d is not None:
                                    existing = state.crawled_urls_with_depth.get(r_norm)
                                    state.crawled_urls_with_depth[r_norm] = _d if existing is None else min(existing, _d)
                                    existing_a = state.additional_urls_with_depth.get(r_norm)
                                    state.additional_urls_with_depth[r_norm] = _d if existing_a is None else min(existing_a, _d)
                            r_redir = getattr(r, 'redirected_url', None)
                            if r_redir:
                                r_redir_norm = _normalize_url(r_redir)
                                seen_crawled_urls.add(r_redir_norm)
                                _d = _new_urls.get(r_norm, all_additional_urls.get(r_norm)) if r_norm else None
                                if _d is not None:
                                    existing = state.crawled_urls_with_depth.get(r_redir_norm)
                                    state.crawled_urls_with_depth[r_redir_norm] = _d if existing is None else min(existing, _d)
                                    existing_a = state.additional_urls_with_depth.get(r_redir_norm)
                                    state.additional_urls_with_depth[r_redir_norm] = _d if existing_a is None else min(existing_a, _d)

                    _total_additional += _new_count
                    print(f"[INFO] Pass {_pass}: Crawled {_new_count} pages")

                if _total_additional > 0:
                    print(f"[INFO] Iterative link extraction complete: {len(all_additional_urls)} additional URLs in {_pass} passes")
            
            # ====================================================================
            # STEP 8: Process Remaining Results
            # ====================================================================
            # Most results were processed incrementally during STEP 7
            # (Option B memory fix).  Process any remaining here
            # (e.g. when MAX_DEPTH == 0 skips STEP 7 entirely).
            _remaining = [r for r in state.all_results[_process_cursor:] if r is not None]
            if _remaining:
                _rebuild_merged_depth_maps()
                _remaining.sort(key=lambda r: len(getattr(r, 'url', '') or ''))
                print("-" * 60)
                print(f"[INFO] Processing {len(_remaining)} remaining pages")
                print("-" * 60)
                _process_batch(_remaining)
                del _remaining
            # Perf #14: shut down the shared executor after all processing is done
            if _shared_executor is not None:
                _shared_executor.shutdown(wait=False)
                _shared_executor = None
            print("-" * 60)
            print(f"[INFO] Crawling complete! {state.pages_processed} pages saved")
            print("-" * 60)
            # Release all result references
            for _ri in range(len(state.all_results or [])):
                state.all_results[_ri] = None
            import gc; gc.collect()

            # FIX B: Retry Varnish 503 error pages with 10-second backoff
            # ====================================================================
            if state.varnish_503_retry_queue:
                _retry_config = CrawlerRunConfig(
                    page_timeout=PAGE_TIMEOUT,
                    wait_until='networkidle',
                    scraping_strategy=scraping_strategy,
                    markdown_generator=markdown_generator,
                    excluded_tags=['nav', 'footer', 'header', 'aside', 'script', 'style', 'noscript', 'select', 'option'],
                    excluded_selector=excluded_selector_str,
                    word_count_threshold=5,
                    remove_forms=True,
                    cache_mode='bypass',
                    locale=FORCE_LOCALE,
                    verbose=True
                )
                _retry_saved, _retry_still_503, _retry_pages, _retry_child_urls = await _content_extraction.retry_varnish_503_pages(
                    crawler=crawler,
                    retry_queue=state.varnish_503_retry_queue,
                    retry_config=_retry_config,
                    varnish_503_markers=_varnish_503_markers,
                    crawled_urls_with_depth_merged=state.crawled_urls_with_depth_merged,
                    additional_urls_with_depth_merged=state.additional_urls_with_depth_merged,
                    max_depth=MAX_DEPTH,
                    output_dir=OUTPUT_DIR,
                    seen_final_urls=state.seen_final_urls,
                    all_successful_urls=state.all_successful_urls,
                    all_urls_by_depth=state.all_urls_by_depth,
                    login_filter_fn=should_skip_login_auth_url,
                    ui_only_query_params=config.ui_only_query_params,
                    content_critical_params=config.content_critical_params,
                    scope_filter_fn=_is_allowed_crawl_url,
                    exclusion_re=_exclusion_re,
                )
                state.pages_processed += _retry_pages

                # ----------------------------------------------------------------
                # Crawl child URLs discovered from successfully retried 503 pages.
                # These children were never found during BFS because the parent
                # page returned 503 under concurrent load.
                # ----------------------------------------------------------------
                if _retry_child_urls:
                    # Filter out URLs already crawled or already seen
                    _rc_batch = []
                    for _rc_norm, _rc_depth in _retry_child_urls.items():
                        if _rc_norm in seen_crawled_urls:
                            continue
                        if should_skip_login_auth_url(_rc_norm):
                            continue
                        if should_skip_query_param_duplicate(_rc_norm, state.seen_normalized_urls):
                            continue
                        _rc_dedup = normalize_url_for_dedup(_rc_norm)
                        if _rc_dedup and _rc_dedup in state.seen_final_urls:
                            continue
                        _rc_batch.append(_rc_norm)
                        # Register depth so assign_page_depth() picks it up
                        state.additional_urls_with_depth[_rc_norm] = _rc_depth

                    if _rc_batch:
                        print(f'[503-RETRY] Crawling {len(_rc_batch)} child URL(s) discovered from retried pages...')
                        _rebuild_merged_depth_maps()
                        try:
                            _rc_dispatcher = MemoryAdaptiveDispatcher(
                                max_session_permit=50,
                                rate_limiter=RateLimiter(
                                    #base_delay=(0.2, 2.0), max_delay=16.0, base*2^(retry_number)
                                    base_delay=(0.05, 0.5), max_delay=60.0,
                                    max_retries=3, rate_limit_codes=[429]
                                )
                            )
                            _rc_results = await crawler.arun_many(
                                _rc_batch, config=_additional_cfg, dispatcher=_rc_dispatcher
                            )
                            _rc_saved = 0
                            for _rc_item in _rc_results:
                                _rc_items = _rc_item if isinstance(_rc_item, list) else ([_rc_item] if _rc_item else [])
                                for _rc_r in _rc_items:
                                    if not _rc_r or not getattr(_rc_r, 'url', None):
                                        continue
                                    if not _is_allowed_crawl_url(_rc_r.url):
                                        continue
                                    if _exclusion_re and _exclusion_re.search(_rc_r.url):
                                        continue
                                    state.all_results.append(_rc_r)
                                    _rc_saved += 1
                                    # Update depth maps
                                    _rc_r_norm = _normalize_url(_rc_r.url)
                                    if _rc_r_norm:
                                        seen_crawled_urls.add(_rc_r_norm)
                                        _rc_d = _retry_child_urls.get(_rc_r_norm)
                                        if _rc_d is not None:
                                            _ex = state.crawled_urls_with_depth.get(_rc_r_norm)
                                            state.crawled_urls_with_depth[_rc_r_norm] = _rc_d if _ex is None else min(_ex, _rc_d)
                                            _ex_a = state.additional_urls_with_depth.get(_rc_r_norm)
                                            state.additional_urls_with_depth[_rc_r_norm] = _rc_d if _ex_a is None else min(_ex_a, _rc_d)
                                    _rc_redir = getattr(_rc_r, 'redirected_url', None)
                                    if _rc_redir:
                                        _rc_rn = _normalize_url(_rc_redir)
                                        if _rc_rn:
                                            seen_crawled_urls.add(_rc_rn)

                            # Process the retry-child results through STEP 8
                            if _rc_saved > 0:
                                _rebuild_merged_depth_maps()
                                _rc_remaining = [r for r in state.all_results[_process_cursor:] if r is not None]
                                if _rc_remaining:
                                    _rc_remaining.sort(key=lambda r: len(getattr(r, 'url', '') or ''))
                                    _process_batch(_rc_remaining)
                                    for _ri in range(_process_cursor, len(state.all_results)):
                                        state.all_results[_ri] = None
                                    _process_cursor = len(state.all_results)
                                    del _rc_remaining
                                    import gc; gc.collect()
                            print(f'[503-RETRY] Crawled {_rc_saved} child page(s) from retried 503 parents.')
                            state.pages_processed += _rc_saved
                        except Exception as _rc_err:
                            print(f'[503-RETRY] Error crawling child URLs: {_rc_err}')
                            state.all_errors.append({
                                'url': '503-retry-child-crawl',
                                'error': str(_rc_err),
                                'error_type': type(_rc_err).__name__,
                                'timestamp': datetime.now().isoformat(),
                                'note': 'Failed to crawl child URLs discovered from retried 503 pages'
                            })


            # ====================================================================
            # PHASE 1 FIX: CRITICAL - Final checkpoint save and aggressive memory cleanup
            # ====================================================================
            _final_memory_cleanup(state, results_metadata)

            # ====================================================================
            # STEP 9: Save URLs by Depth
            # ====================================================================
            depth_summary, _, _ = _checkpoint.save_urls_by_depth(
                state.all_urls_by_depth, links_found_vs_crawled, LOG_DIR,
                ROOT_URLS, MAX_DEPTH)

            # Save per-URL metadata (Last-Modified dates) for incremental updates
            if url_metadata_store:
                _meta_path = LOG_DIR / "url_metadata.json"
                _existing_meta = {}
                if _meta_path.exists():
                    try:
                        with open(_meta_path) as _mf:
                            _existing_meta = json.load(_mf)
                    except Exception:
                        pass
                _existing_meta.update(url_metadata_store)
                with open(_meta_path, 'w') as _mf:
                    json.dump(_existing_meta, _mf, indent=2)
                print(f"[METADATA] Saved {len(url_metadata_store)} URL metadata entries → {_meta_path}")

            # ====================================================================
            # STEP 10: Save Error Log (handled in finally block to ensure it
            #          runs even on crash/interrupt — see below)
            # ====================================================================
            
            # ====================================================================
            # Final Summary
            # ====================================================================
            _print_crawl_summary(state, dedup_enabled, start_time=start_time)
    
    except Exception as cleanup_error:
        # Handle errors during browser startup or cleanup
        error_str = str(cleanup_error)
        is_startup_error = 'ENOSPC' in error_str or 'no space left' in error_str.lower()
        
        if is_startup_error:
            # Critical error: Browser cannot start due to disk space
            print(f"\n[ERROR] Browser startup failed: {error_str}")
            print(f"[ERROR] Cannot proceed with crawling - disk space issue")
            print(f"[INFO] Files saved: {len(state.all_successful_urls)} pages")
            print(f"\n[SOLUTION] Free up disk space and try again:")
            print(f"  - Check /tmp directory: df -h /tmp")
            print(f"  - Clean up temporary files")
            print(f"  - Check available space: df -h")
            
            state.all_errors.append({
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
            print(f"[INFO] Files saved: {len(state.all_successful_urls)} pages")
            
            state.all_errors.append({
                'url': 'Browser Cleanup',
                'error': cleanup_error_msg,
                'timestamp': datetime.now().isoformat(),
                'note': 'This error occurred during browser cleanup after crawling completed. All pages were successfully crawled and saved.'
            })
    
    finally:
        await _url_utils.close_redirect_session()
        _save_error_log_and_exit_checkpoint(state)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
# This runs when you execute: python crawl_desy_simple.py

if __name__ == "__main__":
    # asyncio.run() is needed because crawl_site() is an async function
    # Async functions allow the crawler to fetch multiple pages simultaneously
    asyncio.run(crawl_site())
