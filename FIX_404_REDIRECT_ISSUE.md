# Fix: 404 Redirect Issue for Specific URLs

## Issue
Three specific URLs were being fetched and scraped successfully (FETCH ✓, SCRAPE ✓, COMPLETE ✓), but were redirecting to `/404/index_eng.html` and not producing markdown files:
- `https://www.desy.de/aktuelles/veranstaltungen/index_ger.html`
- `https://www.desy.de/ueber_desy/leitende_wissenschaftler/christian_schwanenberger/index_ger.html`
- `https://www.desy.de/career/contact/index_eng.html`

## Root Cause
The code was normalizing URLs by removing the `www.` prefix before crawling (e.g., `www.desy.de` → `desy.de`). The server at `desy.de` (without www) has bot detection that redirects these specific URLs to 404, while the server at `www.desy.de` (with www) works correctly.

## Solution
Changed the code to use the original URL (with `www.`) for the actual HTTP crawl, while keeping URL normalization only for internal tracking, deduplication, and domain matching in post-processing. This ensures:
- Seed URLs are crawled with their original format (`www.desy.de`)
- Additional URLs extracted from HTML keep their original format
- Normalization is still used for deduplication and checkpoint tracking (which don't trigger HTTP requests)

## Changes Made
1. **Main crawl URL** (line 4878): Changed from `normalized_url = _normalize_url(root_url)` to `crawl_url = root_url`
2. **Additional URLs extraction** (lines 4944-4951): Store original URL format for crawling, use normalized only for deduplication
3. **Error handling**: Updated to use `crawl_url` instead of `normalized_url`

## Status
✅ **RESOLVED** - All three problematic URLs now crawl successfully without 404 redirects.
