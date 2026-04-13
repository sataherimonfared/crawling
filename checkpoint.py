"""Checkpoint save/load utilities for DESY web crawler.

Extracted from crawl_desy_all_urls.py (Step 6 of refactoring plan).
Handles serialisation of crawler state to disk for crash recovery.
"""

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# CrawlState  (extracted Step 7 Part 2a)
# ---------------------------------------------------------------------------

@dataclass
class CrawlState:
    """Owns all mutable tracking state for a crawl run.

    Checkpoint round-trip
    ---------------------
    ``CrawlState.from_checkpoint(data)`` builds a state from checkpoint dict
    (as returned by ``load_checkpoint``).

    ``state.to_checkpoint()`` serialises back to a dict suitable for
    ``save_checkpoint``.

    The fields mirror the variables currently scattered across the top of
    ``crawl_site()``.  Migrating ``crawl_site()`` to use ``CrawlState``
    instead of bare locals is a separate follow-up step.
    """

    # -- persisted in checkpoint -----------------------------------------

    all_errors: list = field(default_factory=list)
    all_successful_urls: list = field(default_factory=list)
    all_urls_by_depth: dict = field(default_factory=dict)
    seen_final_urls: set = field(default_factory=set)
    additional_urls_with_depth: dict = field(default_factory=dict)
    crawled_urls_with_depth: dict = field(default_factory=dict)
    pages_processed: int = 0
    max_depth_crawled: int = 0
    seed_urls_processed: set = field(default_factory=set)

    # -- runtime-only (not persisted) ------------------------------------

    group1_skipped_count: int = 0
    group2_skipped_count: int = 0
    group4_skipped_count: int = 0
    seen_normalized_urls: set = field(default_factory=set)
    duplicate_urls: set = field(default_factory=set)
    all_results: list = field(default_factory=list)
    total_results_crawled: int = 0
    seen_content_hashes: dict = field(default_factory=dict)
    duplicate_count: int = 0
    varnish_503_retry_queue: list = field(default_factory=list)

    # -- merged depth maps (populated after iterative discovery) ---------

    additional_urls_with_depth_merged: dict = field(default=None)
    crawled_urls_with_depth_merged: dict = field(default=None)

    # ------------------------------------------------------------------ #

    @classmethod
    def from_checkpoint(cls, checkpoint: dict) -> 'CrawlState':
        """Build a ``CrawlState`` from a checkpoint dict.

        Missing keys fall back to sensible defaults (empty collections / 0).
        """
        return cls(
            all_errors=checkpoint.get('all_errors', []),
            all_successful_urls=checkpoint.get('all_successful_urls', []),
            all_urls_by_depth=checkpoint.get('all_urls_by_depth', {}),
            seen_final_urls=checkpoint.get('seen_final_urls', set()),
            additional_urls_with_depth=checkpoint.get('additional_urls_with_depth', {}),
            crawled_urls_with_depth=checkpoint.get('crawled_urls_with_depth', {}),
            pages_processed=checkpoint.get('pages_processed', 0),
            max_depth_crawled=checkpoint.get('max_depth_crawled', 0),
            seed_urls_processed=checkpoint.get('seed_urls_processed', set()),
        )

    def to_checkpoint(self) -> dict:
        """Serialise to a dict suitable for ``save_checkpoint``.

        Uses the merged depth maps when available (so the next resume
        has correct depths), falling back to the un-merged maps.
        """
        return {
            'seen_final_urls': self.seen_final_urls,
            'all_urls_by_depth': self.all_urls_by_depth,
            'all_successful_urls': self.all_successful_urls,
            'all_errors': self.all_errors,
            'additional_urls_with_depth': (
                self.additional_urls_with_depth_merged
                if self.additional_urls_with_depth_merged is not None
                else self.additional_urls_with_depth
            ),
            'crawled_urls_with_depth': (
                self.crawled_urls_with_depth_merged
                if self.crawled_urls_with_depth_merged is not None
                else self.crawled_urls_with_depth
            ),
            'pages_processed': self.pages_processed,
            'max_depth_crawled': self.max_depth_crawled,
            'seed_urls_processed': self.seed_urls_processed,
        }


def save_checkpoint(checkpoint_data: dict, checkpoint_file: Path) -> bool:
    """
    Save checkpoint to disk for crash recovery.
    
    Args:
        checkpoint_data: Dictionary containing:
            - seen_final_urls: Set of processed URLs
            - all_urls_by_depth: Dict of URLs organized by depth
            - all_successful_urls: List of successfully saved URLs
            - all_errors: List of errors
            - additional_urls_with_depth: Dict of URL -> depth mapping
            - crawled_urls_with_depth: Dict of already crawled URLs with depths
        checkpoint_file: Path to the checkpoint JSON file
    
    Returns:
        True if save successful, False otherwise
    """
    try:
        # Convert sets to lists for JSON serialization
        serializable_data = {
            'timestamp': datetime.now().isoformat(),
            'seen_final_urls': list(checkpoint_data.get('seen_final_urls', set())),
            'all_urls_by_depth': checkpoint_data.get('all_urls_by_depth', {}),
            'all_successful_urls': checkpoint_data.get('all_successful_urls', []),
            'all_errors': checkpoint_data.get('all_errors', []),
            'additional_urls_with_depth': checkpoint_data.get('additional_urls_with_depth', {}),
            'crawled_urls_with_depth': checkpoint_data.get('crawled_urls_with_depth', {}),
            'pages_processed': checkpoint_data.get('pages_processed', 0),
            'max_depth_crawled': checkpoint_data.get('max_depth_crawled', 0),  # Track the max depth that was crawled
            'seed_urls_processed': list(checkpoint_data.get('seed_urls_processed', set())),  # Track which seed URLs were processed
        }
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = checkpoint_file.with_suffix('.tmp')
        tmp_file.write_text(json.dumps(serializable_data, separators=(',', ':')), encoding='utf-8')
        os.replace(str(tmp_file), str(checkpoint_file))
        return True
    except Exception as e:
        print(f"[WARNING] Failed to save checkpoint: {e}")
        return False


def load_checkpoint(checkpoint_file: Path, use_checkpoint: bool) -> dict:
    """
    Load checkpoint from disk if use_checkpoint is True.
    
    Args:
        checkpoint_file: Path to the checkpoint JSON file
        use_checkpoint: Whether checkpoint resumption is enabled
    
    Returns:
        Dictionary with checkpoint data, or empty dict if no checkpoint exists
    """
    if not use_checkpoint:
        return {}
    
    try:
        if checkpoint_file.exists():
            data = json.loads(checkpoint_file.read_text(encoding='utf-8'))
            # Convert lists back to sets where needed
            return {
                'seen_final_urls': set(data.get('seen_final_urls', [])),
                'all_urls_by_depth': data.get('all_urls_by_depth', {}),
                'all_successful_urls': data.get('all_successful_urls', []),
                'all_errors': data.get('all_errors', []),
                'additional_urls_with_depth': data.get('additional_urls_with_depth', {}),
                'crawled_urls_with_depth': data.get('crawled_urls_with_depth', {}),
                'pages_processed': data.get('pages_processed', 0),
                'max_depth_crawled': data.get('max_depth_crawled', 0),
                'seed_urls_processed': set(data.get('seed_urls_processed', [])),
            }
    except Exception as e:
        print(f"[WARNING] Failed to load checkpoint: {e}")
    
    return {}


def extract_result_metadata(result):
    """Extract lightweight metadata from a crawl result.
    
    This function extracts only essential data (URLs, depth, status)
    and is used to keep a record after discarding heavy data (HTML, markdown).
    
    At 200k URLs with 100KB-5MB markdown per page, not clearing
    would consume 20GB+ in RAM. This enables streaming processing.
    
    Returns:
        dict with url, redirected_url, status_code, success flag
    """
    if not result:
        return None
    
    try:
        html = result.html if hasattr(result, 'html') and result.html else ''
        m = re.search(r'<meta\s+name=["\']Last-Modified["\']\s+content=["\']([^"\']+)["\']',
                      html, re.IGNORECASE)
        meta_last_modified = m.group(1).strip() if m else None
        return {
            'url': getattr(result, 'url', None),
            'redirected_url': getattr(result, 'redirected_url', None),
            'status_code': getattr(result, 'status_code', None),
            'success': getattr(result, 'success', None),
            'html_length': len(html),
            'markdown_length': len(result.markdown) if hasattr(result, 'markdown') and result.markdown else 0,
            'meta_last_modified': meta_last_modified,
        }
    except Exception:
        return {'url': getattr(result, 'url', 'unknown'), 'error': 'metadata_extraction_failed'}


def save_urls_by_depth(all_urls_by_depth, links_found_vs_crawled, log_dir,
                       root_urls, max_depth):
    """Save all crawled URLs organized by depth level to a JSON file.

    Args:
        all_urls_by_depth: dict mapping depth-string → list of url entries
            (each entry is either a dict with 'final_url'/'original_url'/'is_redirect'
             or a plain URL string for legacy format).
        links_found_vs_crawled: dict with keys 'total_links_found_in_html',
            'total_urls_crawled', 'links_found_by_page'.
        log_dir: Path to the log directory.
        root_urls: list of seed URL strings.
        max_depth: int, configured max crawl depth.

    Returns:
        (depth_summary dict, total_unique_final_urls count, redirect_count)
    """
    depth_summary = {}
    total_unique_final_urls = set()
    total_unique_original_urls = set()
    redirect_count = 0

    for depth_str, url_entries in all_urls_by_depth.items():
        for entry in url_entries:
            if isinstance(entry, dict):
                if entry.get('final_url'):
                    total_unique_final_urls.add(entry['final_url'])
                if entry.get('original_url'):
                    total_unique_original_urls.add(entry['original_url'])
                if entry.get('is_redirect'):
                    redirect_count += 1
            else:
                total_unique_final_urls.add(entry)
                total_unique_original_urls.add(entry)

        unique_final_urls_list = []
        seen_in_depth = set()
        for entry in url_entries:
            if isinstance(entry, dict):
                final_url = entry.get('final_url')
                if final_url and final_url not in seen_in_depth:
                    unique_final_urls_list.append(final_url)
                    seen_in_depth.add(final_url)
            else:
                if entry and entry not in seen_in_depth:
                    unique_final_urls_list.append(entry)
                    seen_in_depth.add(entry)

        depth_summary[depth_str] = {
            'count': len(url_entries),
            'unique_final_urls': unique_final_urls_list,
            'url_entries': url_entries,
        }

    urls_by_depth_file = Path(log_dir) / "urls_by_depth.json"
    urls_by_depth_data = {
        'timestamp': datetime.now().isoformat(),
        'root_urls': list(root_urls),
        'max_depth': max_depth,
        'total_urls': sum(len(urls) for urls in all_urls_by_depth.values()),
        'total_unique_final_urls': len(total_unique_final_urls),
        'total_unique_original_urls': len(total_unique_original_urls),
        'redirects_detected': redirect_count,
        'urls_by_depth': depth_summary,
        'link_analysis': {
            'total_links_found_in_html': links_found_vs_crawled['total_links_found_in_html'],
            'total_urls_crawled': links_found_vs_crawled['total_urls_crawled'],
            'average_links_per_page': links_found_vs_crawled['total_links_found_in_html'] / max(links_found_vs_crawled['total_urls_crawled'], 1),
            'sample_pages_with_links': links_found_vs_crawled['links_found_by_page'][:20],
        },
    }
    urls_by_depth_file.write_text(json.dumps(urls_by_depth_data, indent=2), encoding="utf-8")

    print("-" * 60)
    print(f"[DEPTH SUMMARY] URLs by depth:")
    for depth_str in sorted(depth_summary.keys(), key=int):
        count = depth_summary[depth_str]['count']
        print(f"  Depth {depth_str}: {count} URLs")
    print(f"[DEPTH FILE] Saved to {urls_by_depth_file}")

    return depth_summary, len(total_unique_final_urls), redirect_count


def save_error_log(all_errors, all_successful_urls, error_log_file,
                   total_results_crawled=0, all_results=None):
    """Save all failed URLs with error reasons to a JSON file.

    Args:
        all_errors: list of error dicts with 'url', 'error', 'timestamp' etc.
        all_successful_urls: list of successfully saved URL strings.
        error_log_file: Path to the error log JSON file.
        total_results_crawled: int count of total results crawled (for finally block).
        all_results: list (fallback for count if total_results_crawled is 0).
    """
    if all_errors:
        timeout_errors = [
            e for e in all_errors
            if 'timeout' in str(e.get('error', '')).lower()
            or 'timed out' in str(e.get('error', '')).lower()
        ]
        other_errors = [e for e in all_errors if e not in timeout_errors]

        error_log = {
            'timestamp': datetime.now().isoformat(),
            'total_errors': len(all_errors),
            'total_successful': len(all_successful_urls),
            'timeout_errors': len(timeout_errors),
            'other_errors': len(other_errors),
            'timeout_urls': [
                {'url': e.get('url'), 'error': e.get('error'),
                 'timestamp': e.get('timestamp')}
                for e in timeout_errors
            ],
            'errors': all_errors,
        }
    else:
        error_log = {
            'timestamp': datetime.now().isoformat(),
            'total_errors': 0,
            'total_successful': len(all_successful_urls),
            'errors': [],
        }

    error_log_file = Path(error_log_file)
    error_log_file.write_text(json.dumps(error_log, indent=2), encoding="utf-8")

    if all_errors:
        print("-" * 60)
        print(f"[ERROR LOG] Saved {len(all_errors)} errors to {error_log_file}")
