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

| Technique | What it does | Why it matters |
|-----------|-------------|----------------|
| **Async I/O** | Crawls multiple pages at the same time using `asyncio` | 10x faster than crawling one page at a time |
| **BFS Crawling** | Follows links level by level (depth 0 → 1 → 2 → …) | Systematic coverage — nothing is missed |
| **Manual Link Extraction** | Re-reads raw HTML to find links BFS missed | BFS skips links in nav/footer/header; this catches them |
| **Checkpoint/Resume** | Saves crawler state to JSON every 1,000 pages | Job can crash or hit SLURM time limit and resume later |
| **5-Layer Filtering** | Blocks login, error, print-view, RSS, and duplicate URLs | Avoids downloading 10,000+ junk pages |
| **Redirect Handling** | Checks where a URL redirects to before queuing it | Prevents fetching pages on excluded domains |
| **Table Extraction** | Preserves links, emails, and phone numbers in table cells | Crawl4AI loses these; custom extractor keeps them |
| **Content Dedup** | URL normalisation + SHA-256 content hashing | Same page under 5 different URLs → saved only once |
| **Anti-bot & JS** | Playwright stealth mode with full JavaScript rendering | Works on JS-heavy pages with bot detection |
| **SLURM** | 72-hour background runs on HPC clusters | Handles 200k-page crawls unattended |

**Scale:** Up to 200k pages, 10 concurrent browser tasks (configurable), checkpoint every 1,000 pages.

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

> **Key idea:** Without filtering, the crawler would waste time downloading login pages, error pages, PDF files, print-view duplicates, and RSS feeds. The 5-layer pipeline blocks unwanted URLs **before** they consume bandwidth — and double-checks **after** crawling in case anything slipped through.

**Why it matters:** On a 200k-page site, even 5% noise means 10,000 junk files. Filtering keeps the output clean and the crawl fast.

| When | Filter | What it blocks | Example |
|------|--------|----------------|----------|
| **Before fetching** | 60 BFS exclusion patterns | PDFs, calendar files, login paths, print-views, RSS | `/login_form`, `?printversion=1` |
| **Before queuing** | GROUP 1 — Login/auth | SSO pages, admin URLs | `sso.desy.de`, `/acl_users` |
| **Before queuing** | GROUP 2 — Error pages | 404, 503, maintenance | `/404`, `/cgi-bin` |
| **Before queuing** | GROUP 4 — Query-param dedup | URLs that differ only in UI params | `?lang=en` vs `?lang=de` (same content) |
| **After crawling** | 7-stage post-crawl filter | Scope, login-wall content, blank pages, Varnish 503, printversion | Pages with <50 chars of body text |
| **At every append** | Exclusion guard at 4 code points | Anything matching exclusion patterns | Safety net — catches edge cases |

---

## Extracted Modules

> **Key idea:** The original 9,400-line single file was hard to test and debug. I split it into 6 focused modules, each with a clear responsibility and its own test suite.

| Module | What it does | Why it exists |
|--------|-------------|---------------|
| `url_utils.py` | URL normalisation, domain checks, redirect resolution, login/error/dedup filters | Centralises all URL logic — used by both BFS and manual extraction |
| `table_processing.py` | HTML table extraction preserving links, emails, phone numbers | Crawl4AI loses links in table cells; this module fixes that |
| `content_extraction.py` | Indico events, contacts, content hashing, result filtering, depth assignment | Handles DESY-specific page types and the core dedup/depth logic |
| `markdown_cleanup.py` | Post-processing: remove breadcrumbs, broken tables, duplicate paragraphs | Raw markdown from Crawl4AI contains noise; cleanup produces cleaner output |
| `checkpoint.py` | `CrawlState` dataclass, checkpoint save/load, error logging | Makes the crawler crash-safe — state is serialised to JSON every 1,000 pages |

---

## Testing

> **Key idea:** Every filter, depth rule, and dedup decision has a test. 230 tests ensure that changes don't break existing behaviour.

```bash
# Run all tests
python -m unittest discover -v -p "test_*.py"
```

| Test File | Tests | What it verifies |
|-----------|-------|------------------|
| `test_result_url_validation.py` | 26 | URL extraction, 404 detection |
| `test_crawl_state.py` | 25 | Checkpoint save/load, memory cleanup |
| `test_printversion_fixes.py` | 25 | All 5 printversion/siteview fixes |
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

> **Key idea:** After crawling, the markdown files are too large for direct embedding. The chunking pipeline splits them into small, overlapping chunks ready for vector databases and LLM retrieval (RAG).

```bash
python chunking/run_chunking.py --run-id 23 --write-index
```

- **Input:** `.md` files from `desy_crawled/<run_id>/depth_*/`
- **Splitting:** On `##` headings, with sliding-window fallback for long sections
- **Chunk size:** 512 tokens max, 400-word window, 50-word overlap
- **Output:** JSONL with `text` + metadata (source URL, page title, section heading)
- **Cleanup:** Removes boilerplate (External Links, Contact, Career sections)

---

## Technical Deep Dive

### URL Deduplication

> **Key idea:** The same page can appear under many different URLs. Without deduplication, you'd save the same content multiple times. The crawler uses **three layers** of dedup to ensure each page is saved exactly once.

**The problem:** A single DESY page might be reachable via:
- `https://it.desy.de/about/` (base URL)
- `https://www.it.desy.de/about/` (with `www.`)
- `https://it.desy.de/about/index_eng.html` (explicit index)
- `https://it.desy.de/about/?printversion=1` (print view — same content)
- `https://it.desy.de/about/?lang=en&view=standard` (UI params — same content)

**How it's solved (3 layers):**

1. **URL normalisation** — `_normalize_url()` strips `www.`, fragments (`#section`), and trailing slashes. Then `normalize_url_for_dedup()` strips 30 UI-only query params (`printversion`, `lang`, `view`, etc.) while keeping content-changing params (`q`, `page`, `num`).
2. **Sort-by-length trick** — Results are sorted by URL length before processing. Shorter = cleaner URL. The clean URL is always processed first and wins the dedup check; its longer variants are skipped.
3. **Content hashing** — SHA-256 hash of the page body catches byte-identical pages that arrive under completely different URLs.

### Markdown Cleanup Pipeline

> **Key idea:** Crawl4AI's raw markdown output contains navigation breadcrumbs, broken table fragments, and duplicated text. A 3-stage cleanup pipeline removes this noise.

- **Stage 1** (`clean_raw_markdown`) — Removes breadcrumbs, navigation patterns, duplicate lines, broken table fragments.
- **Stage 2** (`deduplicate_markdown_against_tables`) — If the same text appears both in a table and as a paragraph, remove the paragraph (the table version is better formatted).
- **Stage 3** (`post_process_markdown`) — Collapses empty sections, filters malformed tables, strips orphaned separators.
- **Specialised extractors** handle Indico event pages (title/date/location/contributions), PUBDB pages (UI table filtering), and contact pages (label-value pair parsing).

### Depth Assignment

> **Key idea:** Every page gets a "depth" number (0, 1, 2, 3 …) that represents how many clicks away it is from the homepage. This number determines which folder the page is saved in (`depth_0/`, `depth_1/`, etc.).

#### Why two discovery methods?

**The problem:** Crawl4AI's BFS removes `<nav>`, `<footer>`, `<header>`, and `<aside>` from the HTML **before** looking for links. This keeps navigation noise out of the markdown, but it also means BFS never sees links that only appear inside those elements — like a sidebar link to `/desy_in_leichter_sprache/`.

**The solution:** A second pass (manual link extraction) re-reads the **raw, unfiltered HTML** with BeautifulSoup and catches every `<a href>` tag — including ones inside nav/footer/header. This ensures no page is missed.

#### Two depth maps

Because there are two discovery methods, there are two depth maps:

| Map | Filled by | Depth comes from |
|---|---|---|
| `crawled_urls_with_depth` | BFS | Crawl4AI sets `result.metadata['depth']` |
| `additional_urls_with_depth` | Manual extraction | `parent page depth + 1` |

The same URL can appear in both maps with different depths. For example, BFS might find page X at depth 2 (via path A → B → X), while the manual pass finds it at depth 1 (via a direct nav link from the homepage).

#### 5-stage depth resolution

`assign_page_depth()` picks the final depth:

1. **Seed check** — Is it a homepage/seed URL? → depth **0**
2. **Map lookup** — Look in both maps. If found, take the **shallowest** (minimum) value
3. **Metadata fallback** — Read `result.metadata['depth']` from Crawl4AI
4. **Default** — If nothing is known, assume depth **1** (it's at least one click away)
5. **Cap** — Never exceed `MAX_DEPTH`

#### Depth maps survive restarts

The depth maps are saved into the checkpoint. When the crawler resumes after a SLURM restart, it loads the saved maps and merges them with new data — always keeping the shallowest depth ever observed for each URL.

### Key Design Decisions

- **Modular but backward-compatible** — Extracted modules are imported by the main file with thin wrapper functions. Existing code works unchanged; new code can import modules directly.
- **Checkpoint safety** — The `finally` block skips checkpoint save when depth maps are already cleaned up, preventing corruption on SLURM job termination.
- **Defense-in-depth filtering** — Exclusion patterns are checked at all 4 result-append points, not just at the BFS level. Even if one guard fails, the others catch it.
- **Deterministic dedup** — Sorting by URL length guarantees the clean URL always wins over its `?printversion=1` variant, regardless of async crawl order.

---

## Error Handling & Logs

> **Key idea:** The crawler never stops because of a single page error. Every error is logged, and the crawl continues. Transient errors (Varnish 503) are automatically retried.

- **Timeouts** — Logged in `crawl_errors.json`; can retry later with a longer timeout.
- **404 / empty pages** — Skipped (no file written). 404 pages with meaningful content (>100 chars body) are kept.
- **Varnish 503** — Automatically retried after 10-second backoff. Still-failing pages are discarded.
- **Extraction errors** — Caught per page; checkpoint saved so progress isn't lost.
- **Login redirects** — Detected by URL patterns and page content keywords; both original and redirect URLs are checked.

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
