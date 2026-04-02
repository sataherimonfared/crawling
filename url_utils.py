"""
URL utility functions for the DESY Crawl4AI crawler.

Extracted from crawl_desy_all_urls.py (Step 2 of the refactoring plan).
Every function is a pure-ish helper that validates, normalises, filters,
or resolves URLs.  No function reads module-level globals from the main
script — all external data (patterns, domain sets, config values) is
passed explicitly so the functions are independently testable.
"""

import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# aiohttp for async redirect resolution (non-blocking HEAD requests)
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# ---------------------------------------------------------------------------
# Redirect resolution cache
# ---------------------------------------------------------------------------
# Shared across all calls within one process.  Cleared at the start of each
# crawl run by the caller (crawl_site).
_redirect_resolution_cache = {}


# ============================================================================
# URL normalisation
# ============================================================================

def _normalize_url(url):
    """
    Normalize URL for consistent comparison and deduplication.
    - Removes www. prefix
    - Strips fragment (# and everything after)
    - Strips trailing slash from path (treats /page and /page/ as same)
    Query string is preserved.

    Args:
        url: URL string (may be None)

    Returns:
        Normalized URL string or None
    """
    if not url:
        return None
    s = url.split('#', 1)[0]  # remove fragment
    s = s.replace('://www.', '://')  # remove www
    # strip trailing slash from path (before query)
    if '?' in s:
        path_part, query_part = s.split('?', 1)
        path_part = path_part.rstrip('/') or '/'
        s = path_part + '?' + query_part
    else:
        s = s.rstrip('/') or s
    return s


def _normalize_domain(netloc):
    """Return the host portion normalized (lowercased, no www., no port)."""
    if not netloc:
        return ""
    host = netloc.lower().split(':', 1)[0].strip()
    if host.startswith('www.'):
        host = host[4:]
    return host


def _is_desy_domain(netloc):
    """Return True if the normalized host is desy.de or a subdomain thereof."""
    host = _normalize_domain(netloc)
    return host == 'desy.de' or host.endswith('.desy.de')


def _is_allowed_crawl_url(url, allowed_url_prefixes):
    """
    Return True if *url* is inside the configured crawl scope.

    Args:
        url: URL string to check.
        allowed_url_prefixes: Tuple of prefix strings (e.g. ``("https://it.desy.de/",)``).
    """
    if not url:
        return False
    normalized_url = _normalize_url(url) or url
    return any(normalized_url.startswith(prefix) for prefix in allowed_url_prefixes)


# ============================================================================
# URL validation
# ============================================================================

def _is_valid_crawl_url(url, binary_extensions):
    """
    Strict URL validation for crawl attempts.
    Only allows HTTPS URLs with non-empty netloc and blocks binary file
    extensions.

    Args:
        url: URL string to validate.
        binary_extensions: Tuple of lowercase extensions to block
                           (e.g. ``('.gif', '.jpg', ...)``).

    Returns:
        tuple: (is_valid: bool, skip_reason: str or None)
    """
    if not url or not isinstance(url, str):
        return False, "empty_or_not_string"

    parsed = urlparse(url)

    # Only allow HTTPS scheme
    if parsed.scheme not in ('https',):
        if parsed.scheme in ('mailto', 'tel', 'javascript', 'data'):
            return False, f"protocol_{parsed.scheme}"
        return False, "invalid_scheme"

    # Require non-empty netloc
    if not parsed.netloc:
        if url.startswith('#'):
            return False, "fragment_only"
        if not url.startswith('http'):
            return False, "relative_url"
        return False, "missing_netloc"

    # Block binary file extensions (PDF intentionally excluded — has explicit
    # crawl support).
    _path_lower = parsed.path.lower()
    if any(_path_lower.endswith(ext) or (ext + '?') in _path_lower or (ext + '#') in _path_lower
           for ext in binary_extensions):
        return False, "binary_file_extension"

    return True, None


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
    pdf_patterns = [
        r'/pdf/',           # arXiv, many academic sites
        r'/pdfs/',          # Alternative pattern
        r'/document/',      # Some document servers
        r'/file.*\.pdf',    # File paths with .pdf
    ]

    for pattern in pdf_patterns:
        if re.search(pattern, url_clean):
            return True

    return False


# ============================================================================
# Redirect resolution
# ============================================================================

async def _resolve_redirect_final_host(url, timeout=5):
    """
    Resolve redirects and return the final URL's host (normalized: www. removed).
    Used to skip queuing URLs that redirect to EXCLUDED_DOMAINS so we never
    fetch those hosts.
    Returns None on error or if URL is not http(s); caller may then allow the URL.

    PERFORMANCE FIX: Now async using aiohttp — does not block the event loop.
    """
    if not url or not url.strip().lower().startswith(('http://', 'https://')):
        return None
    if url in _redirect_resolution_cache:
        return _redirect_resolution_cache[url]

    if AIOHTTP_AVAILABLE:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    allow_redirects=True,
                    headers={'User-Agent': 'Mozilla/5.0 (compatible; Crawl4AI-redirect-check/1.0)'}
                ) as resp:
                    final = str(resp.url)
            parsed = urlparse(final)
            host = (parsed.netloc or '').replace('www.', '')
            _redirect_resolution_cache[url] = host
            return host
        except Exception:
            _redirect_resolution_cache[url] = None
            return None
    else:
        # Fallback to sync urllib if aiohttp not available
        try:
            import urllib.request
            req = urllib.request.Request(url, method='HEAD',
                                        headers={'User-Agent': 'Mozilla/5.0 (compatible; Crawl4AI-redirect-check/1.0)'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                final = resp.geturl()
            parsed = urlparse(final)
            host = (parsed.netloc or '').replace('www.', '')
            _redirect_resolution_cache[url] = host
            return host
        except Exception:
            _redirect_resolution_cache[url] = None
            return None


# ============================================================================
# GROUP 1 / GROUP 2 / GROUP 4 pre-crawl filters
# ============================================================================

def should_skip_login_auth_url(url, compiled_login_patterns, auth_only_domains):
    """
    GROUP 1 FILTER — skip login / auth / admin URLs.

    Args:
        url: URL string to check.
        compiled_login_patterns: List of pre-compiled regex patterns.
        auth_only_domains: Set of domain strings that serve only auth pages.

    Returns:
        True if URL should be skipped.
    """
    if not url:
        return False

    parsed = urlparse(url)
    domain = _normalize_domain(parsed.netloc)
    if domain in auth_only_domains:
        return True

    url_lower = url.lower()
    for pattern in compiled_login_patterns:
        if pattern.search(url_lower):
            return True

    return False


def should_skip_error_url(url, compiled_error_patterns, error_prone_domains):
    """
    GROUP 2 FILTER — skip known error / maintenance URLs.

    Args:
        url: URL string to check.
        compiled_error_patterns: List of pre-compiled regex patterns.
        error_prone_domains: Set of domain strings that frequently error.

    Returns:
        True if URL should be skipped.
    """
    if not url:
        return False

    parsed = urlparse(url)
    domain = _normalize_domain(parsed.netloc)
    if domain in error_prone_domains:
        return True

    url_lower = url.lower()
    for pattern in compiled_error_patterns:
        if pattern.search(url_lower):
            return True

    return False


def normalize_url_for_dedup(url, ui_only_query_params, content_critical_params):
    """
    Normalize a URL for GROUP 4 deduplication by stripping UI-only query parameters.

    Removes UI-only query params (printversion, embed, lang, session IDs, etc.)
    while keeping content-critical query params (search, pagination, filters).

    Args:
        url: Full URL string.
        ui_only_query_params: Set of param names that are UI-only.
        content_critical_params: Set of param names that affect content.

    Returns:
        Normalized URL with UI-only params removed.
    """
    if not url:
        return url

    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)

        filtered_params = {}
        for key, values in params.items():
            if key not in ui_only_query_params:
                if key in content_critical_params or key not in ui_only_query_params:
                    filtered_params[key] = values

        if not filtered_params:
            normalized_query = ''
        else:
            normalized_query = urlencode(filtered_params, doseq=True)

        normalized_parsed = (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            normalized_query,
            parsed.fragment
        )

        return urlunparse(normalized_parsed)

    except Exception:
        return url


def should_skip_query_param_duplicate(url, seen_normalized_urls,
                                      ui_only_query_params,
                                      content_critical_params):
    """
    GROUP 4 FILTER — skip URLs that differ from an already-seen URL only in
    UI-only query parameters.

    Args:
        url: URL string to check.
        seen_normalized_urls: Mutable set — new normalized URLs are added here.
        ui_only_query_params: Set of param names that are UI-only.
        content_critical_params: Set of param names that affect content.

    Returns:
        True if URL should be skipped.

    Side Effect:
        Adds normalized URL to *seen_normalized_urls* if it is new.
    """
    if not url:
        return False

    normalized = normalize_url_for_dedup(url, ui_only_query_params,
                                         content_critical_params)

    if normalized in seen_normalized_urls:
        return True

    seen_normalized_urls.add(normalized)
    return False
