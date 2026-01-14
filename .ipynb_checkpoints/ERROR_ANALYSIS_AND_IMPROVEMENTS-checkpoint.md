# Error Analysis and Improvements

**Date**: 2025-12-17  
**Error Log**: `desy_crawled/crawl_errors.json`  
**Total Errors**: 38  
**Total Successful**: 1263

---

## Error Analysis Summary

### Error Categories

1. **Timeout Errors**: 16 errors (42%)
   - **Issue**: Pages taking longer than 60 seconds to load
   - **Examples**: 
     - `https://particle-physics.desy.de/events__news/events/index_ger.html`
     - `https://particle-physics.desy.de/events__news/events/kolloquien/index_ger.html`
     - `https://www.desy.de/ueber_desy/organisation/gremien/index_ger.html`
   - **Error Message**: `Timeout 60000ms exceeded`

2. **Indico .ics Errors**: 16 errors (42%)
   - **Issue**: Indico calendar files (.ics) trigger downloads instead of displaying content
   - **Examples**:
     - `https://indico.desy.de/event/51353/event.ics?detail=events`
     - `https://indico.desy.de/event/50122/event.ics?detail=events`
   - **Error Message**: `net::ERR_ABORTED` (download triggered, browser aborted)

3. **Connection Errors**: 6 errors (16%)
   - **Issue**: DNS resolution failures or connection refused
   - **Examples**:
     - `http://fh1.desy.de` (ERR_NAME_NOT_RESOLVED)
     - `http://www-atlas.desy.de` (ERR_CONNECTION_REFUSED)
   - **Note**: These are legitimate failures (servers down or DNS issues)

---

## Solutions Implemented

### 1. ✅ Increased Timeout for Slow Pages

**Problem**: Default timeout (60 seconds) is too short for slow-loading DESY pages.

**Solution**: Increased `page_timeout` in `CrawlerRunConfig`:

```python
PAGE_TIMEOUT = 120000  # 120 seconds (120000ms) - handles slow-loading pages

config = CrawlerRunConfig(
    page_timeout=PAGE_TIMEOUT,  # Increased from 60000ms to 120000ms
    ...
)
```

**Expected Result**: 
- Slow pages like `particle-physics.desy.de/events__news/events/` should now load successfully
- Reduces timeout errors from 16 to ~0-2 (only for extremely slow pages)

**Configuration**:
- Default: 60000ms (60 seconds)
- New: 120000ms (120 seconds)
- Can be increased further if needed: 180000ms (3 minutes)

---

### 2. ✅ Filter Out Indico .ics URLs

**Problem**: Indico .ics files trigger downloads, causing `ERR_ABORTED` errors.

**Solution**: Added `filter_chain` to `BFSDeepCrawlStrategy` to exclude .ics URLs:

```python
from crawl4ai.deep_crawling import RegexURLFilter

# Exclude .ics files and other non-web-page files
exclusion_patterns = [
    r'.*\.ics(\?.*)?$',  # Indico calendar files
    r'.*\.pdf$',         # PDF files
    r'.*\.zip$',         # ZIP files
    r'.*\.docx?$',      # Word documents
    r'.*\.xlsx?$',      # Excel files
]

filter_chain = [RegexURLFilter(pattern, include=False) for pattern in exclusion_patterns]

deep_crawl_strategy = BFSDeepCrawlStrategy(
    ...
    filter_chain=filter_chain  # Exclude problematic URLs
)
```

**Expected Result**:
- .ics URLs are skipped during crawling (not attempted)
- No more `ERR_ABORTED` errors from Indico calendar files
- Reduces errors from 16 to 0 for .ics files

**Note**: If you need event information from Indico, you should:
- Crawl the main Indico event page (not the .ics download link)
- Or use Indico's API to fetch event data directly

---

### 3. ✅ Track and Save URLs by Depth

**Problem**: Need to see how many URLs were found at each depth level.

**Solution**: Track depth from `result.metadata.get('depth', 0)` and save to JSON:

```python
# Track URLs by depth
urls_by_depth = {}  # {0: [urls], 1: [urls], 2: [urls]}

for result in results:
    depth = result.metadata.get('depth', 0) if hasattr(result, 'metadata') else 0
    depth_str = str(depth)
    
    if depth_str not in urls_by_depth:
        urls_by_depth[depth_str] = []
    urls_by_depth[depth_str].append(result.url)

# Save to JSON file
urls_by_depth_data = {
    'timestamp': datetime.now().isoformat(),
    'root_url': ROOT_URL,
    'max_depth': MAX_DEPTH,
    'total_urls': sum(len(urls) for urls in urls_by_depth.values()),
    'urls_by_depth': {
        '0': {'count': len(urls_by_depth['0']), 'urls': urls_by_depth['0']},
        '1': {'count': len(urls_by_depth['1']), 'urls': urls_by_depth['1']},
        '2': {'count': len(urls_by_depth['2']), 'urls': urls_by_depth['2']},
    }
}
```

**Output File**: `desy_crawled/urls_by_depth.json`

**Structure**:
```json
{
  "timestamp": "2025-12-17T...",
  "root_url": "https://www.desy.de",
  "max_depth": 2,
  "total_urls": 1301,
  "urls_by_depth": {
    "0": {
      "count": 1,
      "urls": ["https://www.desy.de"]
    },
    "1": {
      "count": 33,
      "urls": ["https://www.desy.de/about", ...]
    },
    "2": {
      "count": 1267,
      "urls": ["https://www.desy.de/about/team", ...]
    }
  }
}
```

**Benefits**:
- See exactly how many URLs at each depth
- Compare with your original scraper (33 at depth 1, 862 at depth 2)
- Identify if certain depths have more/fewer URLs than expected
- Useful for debugging and analysis

---

## Expected Improvements

### Before (Current State)
- **Timeout Errors**: 16 (42%)
- **Indico .ics Errors**: 16 (42%)
- **Connection Errors**: 6 (16%)
- **Total Errors**: 38
- **Success Rate**: 97.1% (1263/1301)

### After (With Improvements)
- **Timeout Errors**: ~0-2 (only extremely slow pages)
- **Indico .ics Errors**: 0 (filtered out)
- **Connection Errors**: 6 (unchanged - legitimate failures)
- **Total Errors**: ~6-8
- **Expected Success Rate**: 99.4%+ (1293-1295/1301)

---

## Configuration Options

### Adjust Timeout

If you still see timeout errors, increase `PAGE_TIMEOUT`:

```python
# For very slow pages
PAGE_TIMEOUT = 180000  # 3 minutes (180 seconds)

# For extremely slow pages
PAGE_TIMEOUT = 300000  # 5 minutes (300 seconds)
```

**Trade-off**: Higher timeout = longer wait for unresponsive pages

### Adjust URL Filters

To exclude additional URL patterns:

```python
exclusion_patterns = [
    r'.*\.ics(\?.*)?$',  # Calendar files
    r'.*\.pdf$',         # PDFs
    r'.*\.zip$',         # Archives
    r'.*\.docx?$',      # Word docs
    r'.*\.xlsx?$',      # Excel files
    r'.*\.jpg$',        # Images (if you don't want them)
    r'.*\.png$',        # Images
    # Add more patterns as needed
]
```

---

## Files Generated

1. **`desy_crawled/crawl_errors.json`** - Error log (existing)
   - All failed URLs with error reasons
   - Timestamps
   - Error messages

2. **`desy_crawled/urls_by_depth.json`** - Depth summary (NEW)
   - URLs organized by depth level
   - Count per depth
   - Complete URL lists per depth
   - Total statistics

---

## Testing

Run the updated script:

```bash
python crawl_desy_simple.py
```

**Expected Results**:
1. ✅ Fewer timeout errors (0-2 instead of 16)
2. ✅ No Indico .ics errors (filtered out)
3. ✅ `urls_by_depth.json` file created with depth statistics
4. ✅ Higher success rate (99%+)

---

## Recommendations

### For Slow Pages
- **Current**: 120 seconds timeout
- **If still timing out**: Increase to 180-300 seconds
- **Alternative**: Retry failed URLs with longer timeout

### For Indico Events
- **Current**: .ics files are filtered out (skipped)
- **Alternative**: Crawl main Indico event pages instead of .ics download links
- **Example**: 
  - ❌ Skip: `https://indico.desy.de/event/50122/event.ics`
  - ✅ Crawl: `https://indico.desy.de/event/50122/`

### For Connection Errors
- **Current**: These are legitimate failures (servers down, DNS issues)
- **Recommendation**: Keep as errors - they indicate real problems
- **No action needed** - these are expected failures

---

## Summary

All three issues have been addressed:

1. ✅ **Timeout handling**: Increased to 120 seconds
2. ✅ **Indico .ics filtering**: URLs excluded via filter_chain
3. ✅ **Depth tracking**: URLs by depth saved to JSON

The crawler should now have:
- **Higher success rate** (99%+)
- **Better error handling** (timeouts and downloads)
- **Complete depth statistics** (urls_by_depth.json)
