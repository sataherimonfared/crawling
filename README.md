# DESY Website Crawler

A production-grade async web crawler for the DESY IT website (`it.desy.de`), built with [Crawl4AI](https://github.com/unclecode/crawl4ai) 0.8.0, Playwright/Chromium, and Python 3 asyncio.

> **Key idea:** Crawl up to **200,000 web pages**, convert each one to a clean markdown file, organise them by link depth, and make the output ready for RAG / LLM embedding — all running unattended on a SLURM HPC cluster with automatic checkpoint and resume.

---

## Key Contributions

These are the main things **I built and improved** on top of the base Crawl4AI library:

| # | Contribution | Why it matters |
|---|---|---|
| 1 | **Modular architecture** — Refactored 9,400-line monolith into 6 focused modules | Testable, maintainable, each module has a single responsibility |
| 2 | **5-layer URL filtering pipeline** — 4 filter groups + 60 BFS exclusion patterns | Prevents crawling login pages, error pages, print-view duplicates, and RSS feeds — saves bandwidth and avoids noise |
| 3 | **Dual link discovery** — BFS + manual HTML extraction | BFS misses links inside `<nav>`/`<footer>`/`<header>`; manual pass catches them so no page is lost |
| 4 | **Smart deduplication** — URL normalisation + SHA-256 content hashing | The same page can appear under 5+ different URLs; dedup ensures each page is saved exactly once |
| 5 | **5-stage depth assignment** — Seed check → map lookup → metadata → default → cap | Every page gets the correct "clicks from homepage" number, even across restarts |
| 6 | **Crash-safe checkpoint/resume** — `CrawlState` dataclass with `from_checkpoint()` / `to_checkpoint()` | A 72-hour crawl can be interrupted and resumed without losing progress |
| 7 | **Varnish 503 retry** — Automatic retry with backoff for transient backend errors | DESY's Varnish cache occasionally returns 503; retry recovers those pages |
| 8 | **230 unit tests** across 12 test files | Every filter, depth rule, and dedup decision is tested |
| 9 | **Chunking pipeline** — Post-crawl markdown splitting for RAG/embedding | Output is ready for vector databases and LLM retrieval |

---

## What's New (March 2026)

- **Modular architecture** — 9,400-line monolith → 6 modules + 230 tests.
- **`CrawlConfig` / `CrawlState` dataclasses** — All config and state in clean, serialisable objects.
- **5-layer URL filtering** — Login, error, printversion, query-param dedup, BFS exclusion.
- **Printversion / `@@siteview` fix** — 5-part fix so Plone print-view duplicates are never saved.
- **Depth-0 or-chain fix** — Seed URLs (depth 0) no longer misclassified during Varnish 503 retry.
- **Crash-safe checkpoints** — Prevents overwriting valid checkpoint data on SLURM job termination.
- **Varnish 503 retry** — Transient backend errors retried with cache-bypass after backoff.
- **Chunking pipeline** — Post-crawl markdown chunking for embedding/RAG.

---

## Project Structure

```
crawl4ai/
├── crawl_desy_all_urls.py    # Main crawler — BFS deep-crawl, content extraction, markdown output
├── url_utils.py              # URL validation, normalisation, filtering, redirect resolution
├── table_processing.py       # HTML table extraction, link preservation, markdown formatting
├── content_extraction.py     # Indico events, contacts, dedup, result filtering, depth assignment
├── markdown_cleanup.py       # Markdown post-processing, broken-fragment removal, dedup
├── checkpoint.py             # CrawlState dataclass, checkpoint save/load, error/depth logging
├── run_crawler.slurm         # SLURM job submission script
├── monitor_crawler.sh        # Live monitoring helper for SLURM jobs
├── requirements.txt          # Python dependencies (mamba/pip)
├── test_*.py                 # 230 unit tests across 12 test files
└── chunking/                 # Post-crawl markdown chunking for RAG/embedding
    ├── chunker.py            # RegexChunking + sliding-window fallback
    ├── config.py             # Chunking configuration (token limits, overlap, etc.)
    ├── run_chunking.py       # CLI entry point
    ├── sidebar_dedup.py      # Sidebar/boilerplate deduplication
    └── output/               # Chunked JSONL output
```

---

## Technical Overview

| Technique | Description |
|-----------|-------------|
| **Async I/O** | `asyncio` + `AsyncWebCrawler` for concurrent browser tasks without blocking |
| **BFS Crawling** | `BFSDeepCrawlStrategy` — systematic breadth-first coverage (depth 0 → 1 → 2 → …) |
| **Checkpoint/Resume** | `CrawlState` serialised to JSON every 1,000 pages; resume after crash or SLURM time limit |
| **5-Layer Filtering** | Login/auth (GROUP 1), error pages (GROUP 2), printversion/siteview (GROUP 3), RSS/logoff (GROUP 3b), query-param dedup (GROUP 4) |
| **BFS Exclusion Patterns** | 60+ regex patterns fed to Crawl4AI's `FilterChain` to block URLs before fetching |
| **Redirect Handling** | Pre-queue async HEAD resolution via aiohttp; URLs redirecting to excluded hosts are never fetched |
| **Table Extraction** | `LinkPreservingTableExtraction` — preserves links, emails, and phone numbers in table cells |
| **Anti-bot & JS** | Playwright with stealth mode; full JavaScript rendering; configurable locale |
| **Domain Scoping** | Restricted to `ALLOWED_URL_PREFIXES` (default: `https://it.desy.de/`); `www.` normalisation everywhere |
| **Content Dedup** | SHA-256 content hashing + `normalize_url_for_dedup()` query-param stripping |
| **Robust Write Path** | `all_urls_by_depth` updated only after successful file write — counts always match files |
| **SLURM** | `run_crawler.slurm` for 72-hour background runs on HPC clusters |

**Scale:** Up to 200k pages, 10 concurrent browser tasks (configurable), checkpoint every 1,000 pages, memory-conscious (results freed after processing).

---

## How It Works (Simple Overview)

> **Key idea:** The crawler works like opening a website, clicking every link, clicking every link on *those* pages, and so on — up to a configurable depth. Each page is saved as a markdown file in a folder named after its depth.

```
Seed URLs (depth 0)
  │
  ├──→ BFS discovers links on page  ──→  depth 1 pages
  │       │
  │       └──→  depth 2, depth 3 …
  │
  └──→ Manual pass catches links BFS missed (nav/footer/header)
          │
          └──→  also crawled and assigned depth
```

**Step by step:**

1. **Start or resume** — Load checkpoint if one exists, otherwise start fresh.
2. **Crawl seed URLs** — Open each seed page in a headless browser, follow links using BFS up to `MAX_DEPTH`.
3. **Catch missed links** — BFS skips links inside `<nav>`, `<footer>`, `<header>`. A second pass re-reads the raw HTML and catches those links.
4. **Filter** — Before saving, every result passes through a 7-stage filter (scope, login, binary, login-wall, blank, Varnish 503, printversion).
5. **Deduplicate** — The same page can appear under many URLs. URL normalisation + content hashing ensure it's saved only once.
6. **Assign depth** — Each page gets a depth number (0, 1, 2, 3 …) that represents "how many clicks from the homepage."
7. **Save** — One `.md` file per page, organised into `depth_0/`, `depth_1/`, etc.
8. **Checkpoint** — State is saved every 1,000 pages. If the job crashes, it resumes from where it left off.

---

## Configuration

All configuration lives in the `CrawlConfig` dataclass (with module-level constants for backward compatibility).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ROOT_URLS` | `it.desy.de/index_{eng,ger}.html` | Seed URLs to start crawling from |
| `ALLOWED_URL_PREFIXES` | `("https://it.desy.de/",)` | Scope restriction — only crawl URLs matching these prefixes |
| `OUTPUT_DIR` | `desy_crawled/23` | Output directory for depth folders and `.md` files |
| `LOG_DIR` | `OUTPUT_DIR/log` | Log directory for checkpoint, errors, and depth JSON |
| `MAX_DEPTH` | `3` | Maximum link depth (0 = seeds only) |
| `MAX_PAGES` | `200,000` | Maximum total pages to crawl |
| `CONCURRENT_TASKS` | `10` | Number of parallel Playwright browser tasks |
| `PAGE_TIMEOUT` | `180,000 ms` | Page load timeout (3 minutes) |
| `USE_CHECKPOINT` | `True` | Resume from previous checkpoint if available |
| `CHECKPOINT_FREQUENCY` | `1,000` | Save checkpoint every N processed pages |
| `EXCLUDED_DOMAINS` | `fater.desy.de`, `bib-pubdb1.desy.de` | Domains never queued or fetched |
| `CHECK_REDIRECTS_TO_EXCLUDED` | `True` | Resolve redirects before queuing; skip if final host is excluded |
| `UI_ONLY_QUERY_PARAMS` | 30 params | Query params stripped for dedup (e.g. `printversion`, `lang`, `view`) |
| `CONTENT_CRITICAL_PARAMS` | `q`, `page`, `num`, etc. | Query params preserved — different values = different content |

---

## Usage

### Local Run (outside cluster)

```bash
# Install dependencies
pip install -r requirements.txt
# Or use existing mamba environment:
# mamba activate crawl4ai

cd /path/to/crawl4ai
python crawl_desy_all_urls.py
```

### Interactive SLURM Session (DESY Maxwell)

```bash
# 1. Allocate a node
salloc -p maxgpu --time=10:00:00

# 2. SSH to the assigned node
ssh max-???

# 3. Activate environment and run
. mamba-init
mamba activate crawl4ai
cd /path/to/crawl4ai
python crawl_desy_all_urls.py
```

### SLURM Background Job (long run)

```bash
# Submit
sbatch run_crawler.slurm

# Monitor
./monitor_crawler.sh              # auto-finds latest job
tail -f crawler_<jobid>.out       # manual monitoring

# Check status / cancel
squeue -u $USER
scancel <jobid>
```

SLURM defaults (`run_crawler.slurm`): partition `allcpu`, 72 hours, 24 GB RAM, 30 CPUs. Match `--cpus-per-task` to `CONCURRENT_TASKS`.

---

## Output Layout

```
desy_crawled/23/
├── depth_0/                        # Seed pages (2 files for EN + DE)
│   └── it.desy.de_index_eng.html.md
├── depth_1/                        # Pages one click from seeds
├── depth_2/                        # Pages two clicks from seeds
├── depth_3/                        # Pages three clicks from seeds
└── log/
    ├── crawl_checkpoint.json       # Full crawler state for resume
    ├── crawl_errors.json           # Failed URLs with error reasons
    └── urls_by_depth.json          # Per-depth URL lists and counts
```

Each `.md` file is named from the URL path (sanitised, max 200 chars). Filenames use the dedup-normalised URL (UI-only query params stripped) so `?printversion=1` variants get the same clean filename as the base page.

---

## URL Filtering Pipeline

The crawler applies filters at multiple stages to avoid wasting bandwidth on unwanted pages:

| Stage | Group | What it blocks | # Patterns |
|-------|-------|----------------|------------|
| **Pre-crawl (BFS)** | Exclusion patterns | `.pdf`, `.ics`, non-HTTP schemes, login paths, error pages, `@@siteview`, `?printversion=`, RSS/feeds, logoff, excluded domains | ~60 |
| **Pre-queue** | GROUP 1 | Login/auth/admin URLs (`/login`, `/acl_users`, `sso.desy.de`, etc.) | 20 patterns + 4 domains |
| **Pre-queue** | GROUP 2 | Error/maintenance URLs (`/404`, `/503`, `cgi-bin`, etc.) | 12 patterns + 4 domains |
| **Pre-queue** | GROUP 4 | Query-param duplicates (strips `printversion`, `lang`, `view`, etc.) | 30 UI-only params |
| **Post-crawl** | `filter_result_pre_save()` | Scope, login, binary extensions, login-wall content, blank pages, Varnish 503, printversion/siteview | 7 stages |
| **Post-append** | Exclusion guard | Results matching exclusion patterns filtered at every `all_results.append()` site | 4 guard points |

---

## Extracted Modules

The original monolithic script was decomposed into testable modules:

| Module | Lines | Functions | Responsibility |
|--------|-------|-----------|----------------|
| `url_utils.py` | ~320 | 10 | URL normalisation, domain checks, redirect resolution, login/error/dedup filters |
| `table_processing.py` | ~2,700 | 21 | HTML table extraction with link/email preservation, PUBDB detection, markdown formatting |
| `content_extraction.py` | ~1,850 | 24 | Indico events, contacts, soup caching, content hashing, result filtering, depth assignment |
| `markdown_cleanup.py` | ~870 | 8 | Post-processing: separator removal, table dedup, broken fragments, navigation noise |
| `checkpoint.py` | ~340 | 5 + class | `CrawlState` dataclass, checkpoint I/O, error logging, metadata extraction |

All modules are imported by the main file and exposed as thin wrapper functions for backward compatibility.

---

## Testing

230 unit tests across 12 test files:

```bash
# Run all tests
python -m unittest discover -v -p "test_*.py"

# Run a specific test file
python -m unittest test_crawl_state -v
python -m unittest test_printversion_fixes -v

# Run a single test
python -m unittest test_assign_page_depth.TestAssignPageDepth.test_seed_original_url -v
```

| Test File | Tests | Covers |
|-----------|-------|--------|
| `test_crawl_state.py` | 25 | CrawlState defaults, checkpoint round-trip, memory cleanup |
| `test_printversion_fixes.py` | 25 | All 5 printversion/siteview fixes |
| `test_result_url_validation.py` | 26 | URL extraction, 404 detection |
| `test_assign_page_depth.py` | 24 | 5-stage depth assignment |
| `test_filter_result_pre_save.py` | 23 | 7-stage post-crawl filter |
| `test_post_process_markdown.py` | 20 | Markdown post-processing |
| `test_clean_raw_markdown.py` | 20 | Raw markdown cleanup |
| `test_deduplicate_markdown.py` | 20 | Table-vs-markdown dedup |
| `test_count_internal_links.py` | 19 | Internal link counting |
| `test_retry_varnish_503.py` | 14 | Varnish 503 retry logic |
| `test_extract_tables_images.py` | 12 | Table/image extraction |
| `test_incremental_features.py` | 2 | Integration (requires crawl4ai runtime) |

---

## Chunking Pipeline (Post-Crawl)

The `chunking/` directory provides a post-processing pipeline that splits crawled markdown files into chunks suitable for embedding and RAG indexing:

```bash
python chunking/run_chunking.py --run-id 23 --write-index
```

- Reads `.md` files from `desy_crawled/<run_id>/depth_*/`
- Splits on `##` headings (RegexChunking) with sliding-window fallback for oversized sections
- Token limit: 512 tokens per chunk, 400-word window, 50-word overlap
- Outputs JSONL with `text` + metadata (source URL, page title, section heading)
- Removes boilerplate sections (External Links, Contact, Career, etc.)

---

## Technical Notes

### URL Deduplication

- URLs are normalised in two stages: `_normalize_url()` strips `www.`, fragments, and trailing slashes; `normalize_url_for_dedup()` additionally strips 30 UI-only query params (e.g. `printversion`, `lang`, `view`, `embed`) while preserving content-critical params like `q`, `page`, `num`.
- Before result processing, `state.all_results` is sorted by URL length so the shorter (cleaner) variant is always processed first and wins the dedup check. SHA-256 content hashing catches byte-identical pages that arrive under different URLs.

### Markdown Cleanup Pipeline

- Raw markdown passes through three stages: `clean_raw_markdown()` removes breadcrumbs, navigation patterns, duplicate lines, and broken table fragments; `deduplicate_markdown_against_tables()` removes text that duplicates the DOM-ordered table content; `post_process_markdown()` collapses empty sections, filters malformed tables, and strips orphaned separators.
- Specialised extractors handle Indico event pages (structured title/date/location/contributions), PUBDB pages (UI table filtering), and contact pages (label-value pair parsing).

### Depth Assignment

#### Why two discovery methods?

Crawl4AI's BFS (`BFSDeepCrawlStrategy`) removes `<nav>`, `<footer>`, `<header>`, and `<aside>` elements from the HTML **before** it looks for links. This is good for keeping navigation noise out of the final markdown, but it means BFS never sees links that live exclusively inside those elements — for example, a sidebar link to `/desy_in_leichter_sprache/` that only appears in `<nav>`.

To avoid missing those links, a second pass (manual link extraction) re-reads the **raw, unfiltered HTML** of every already-crawled page with BeautifulSoup and collects all `<a href>` tags — including ones inside nav/footer/header. Any new links that BFS missed are then crawled as single pages.

#### Two depth maps

Each discovery method records the depth at which it found a URL:

| Map | Populated by | Source of depth value |
|---|---|---|
| `crawled_urls_with_depth` | BFS results | `result.metadata['depth']` set by Crawl4AI |
| `additional_urls_with_depth` | Manual link extraction | `source_depth + 1` (parent page's depth + 1) |

The same URL can appear in both maps with different depths (e.g., BFS found it at depth 2 via one path, manual extraction found it at depth 1 via a direct sidebar link from a seed page).

#### How `assign_page_depth()` resolves the final depth

The function resolves depth in 5 stages:

1. **Seed check** — if the URL is a seed (root) URL → depth **0**.
2. **Map lookup** — look up the URL in both maps (trying both original and final redirect URL). If found, take the **shallowest** (minimum) value.
3. **Metadata fallback** — `result.metadata['depth']` or `result.depth`.
4. **Default** — non-seed pages with no depth info default to **1**.
5. **Cap** — clamp to `MAX_DEPTH`.

#### Checkpoint persistence

The `*_merged` depth maps accumulate across SLURM job restarts via checkpoint round-trips — `CrawlState.to_checkpoint()` saves merged maps, and `from_checkpoint()` restores them, always keeping the shallowest-ever observed depth for each URL.

### Key Design Decisions

- **Modular but backward-compatible** — Extracted modules (`url_utils.py`, `table_processing.py`, etc.) are imported by the main file, which exposes thin wrapper functions. Existing call sites are unchanged; new code can import modules directly.
- **Checkpoint safety** — The `finally` block skips checkpoint save when depth maps have been nullified by memory cleanup, preventing corruption of valid checkpoint data on SLURM job termination.
- **Defense-in-depth filtering** — BFS and `arun_many` results are filtered through `exclusion_patterns` at all 4 result-append points, not just at the BFS filter-chain level. This ensures correctness even when the Crawl4AI `FilterChain` import falls back to list mode.
- **Deterministic dedup** — Sorting `all_results` by URL length before processing guarantees the base URL (without `?printversion=1`) always wins over its parameterised variant, regardless of async crawl order.

---

## Error Handling & Logs

- **Timeouts** — Logged in `crawl_errors.json` with `is_timeout: true`; can retry with `PAGE_TIMEOUT_EXTENDED`.
- **404 / empty pages** — Skipped (no file written, no count in `urls_by_depth.json`). 404 pages with meaningful content (>100 chars) are kept.
- **Varnish 503** — Queued for retry with cache-bypass after 10-second backoff. Still-503 pages are discarded.
- **Extraction errors** — Caught per result; error appended to `all_errors`; checkpoint saved so progress isn't lost.
- **Login redirects** — Detected by URL patterns and content keywords; both original and final URLs are checked.

---

## Troubleshooting

- **Count in `urls_by_depth.json` ≠ number of files**
  With the current code they should match (append to `all_urls_by_depth` only after successful write). If not, check for an interrupted previous run.

- **Job killed by SLURM time limit**
  The `finally` block saves a checkpoint on exit. Resume with `USE_CHECKPOINT=True`. Increase `--time` in `run_crawler.slurm` if needed.

- **Too slow**
  Increase `CONCURRENT_TASKS` and match `--cpus-per-task` in SLURM. Current default is 10; the HPC node supports up to 96 cores.

- **Many "Invalid URL" or skip messages**
  This is normal — the filter pipeline is working. Adjust `exclusion_patterns` or `EXCLUDED_DOMAINS` to include/exclude more URLs.

- **`NameError: name 'dataclass' is not defined`**
  The `from dataclasses import dataclass, field` import is missing. Add it to the imports at the top of the file.

---

## Contact

Questions or issues: **sara.taherimonfared@gmail.com**

---

## License & Compliance

Use in line with DESY's robots.txt and terms of service. The script is provided as-is.
