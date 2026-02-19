# DESY Website Crawler

A production-grade web crawler for DESY (*.desy.de) built with [Crawl4AI](https://github.com/unclecode/crawl4ai). It uses **async Python**, **BFS crawling**, **checkpoint/resume**, **custom content extraction**, and **SLURM** for long runs. Built to handle 100k+ URLs with controlled memory use and resume after crashes or time limits.

---
## to get CRAWL4AI version

python -c "import crawl4ai.__version__ as v; print(v.__version__)"

---

## Technical Overview

**Techniques and technologies used:**

- **Async I/O** — `asyncio` + `AsyncWebCrawler` for high concurrency (e.g. 30 parallel browser tasks) without blocking.
- **Breadth-First Search (BFS)** — Systematic multi-depth crawling via Crawl4AI’s `BFSDeepCrawlStrategy` (depth 0 → 1 → 2 …).
- **Checkpoint & resume** — Progress (seen URLs, depth maps, errors) saved to JSON; job can be stopped (time limit, `scancel`) and resumed without re-crawling.
- **Depth assignment** — Single source of truth: seed = 0; then merged “crawled” and “additional” URL→depth maps (current run overwrites checkpoint); then metadata; fallback; cap at `MAX_DEPTH`.
- **Filter chain** — Regex-based URL filter (Crawl4AI) to exclude non-HTTP schemes, file types (e.g. `.pdf`, `.ics`), and specific hosts (e.g. `fater.desy.de`, `bib-pubdb1.desy.de`) before links are queued.
- **Redirect handling** — Tracks original vs final URL; optional **pre-queue redirect resolution** (HEAD) so URLs that redirect to excluded hosts are never fetched.
- **Custom table extraction** — `LinkPreservingTableExtraction` extends Crawl4AI to keep links and `mailto:` in table cells and convert them to markdown.
- **Anti-bot & JS** — Playwright with stealth options and headless mode; full JavaScript rendering.
- **Domain scoping** — Restricts to `*.desy.de`; explicit exclusion set for chosen subdomains; same normalization (e.g. strip `www.`) everywhere.
- **Robust write path** — `all_urls_by_depth` is updated only **after** a successful file write, so JSON counts match actual `.md` files; guards around `main_content` to avoid `NoneType` during HTML extraction.
- **SLURM** — Script (`run_crawler.slurm`) for cluster submission (CPUs, memory, time, partition) so long crawls run in the background.

**Scale:** Configurable up to 200k pages, checkpoint every 1000 pages, memory-conscious (results processed and cleared; metadata kept).

---

## How It Works

High-level pipeline:

1. **Start / resume**
   - If `USE_CHECKPOINT` is on, load last checkpoint (seen URLs, depth maps, errors). Otherwise start from scratch.

2. **Browser & strategy**
   - Start a single browser (Playwright) with anti-bot and headless settings.
   - Set BFS strategy: follow links level by level (depth 0 = seeds only, then 1, then 2, …) up to `MAX_DEPTH`.

3. **Crawl seeds**
   - For each URL in `ROOT_URLS`, call the crawler with BFS. It returns many results (seed + depth 1 + …).
   - Optionally, from the first seed result we do **extra link extraction** (e.g. nav/footer), resolve redirects for excluded hosts, and crawl a batch of “additional” single-page URLs so we don’t miss links the main strategy skips.

4. **Collect more links from all current results**
   - Parse HTML of every result with BeautifulSoup; collect links that are:
     - Same domain (`*.desy.de`),
     - Not in the excluded set (e.g. `fater.desy.de`, `bib-pubdb1.desy.de`),
     - Not redirecting to excluded hosts (if redirect check is on).
   - Assign depth = source page depth + 1; cap at `MAX_DEPTH`. Crawl these “additional” URLs with the same crawler.

5. **Assign depth and write files**
   - For each result we:
     - Decide depth: seed → 0; else from merged “crawled” map, then “additional” map, then Crawl4AI metadata, then fallback 1; then cap.
     - Build markdown (from Crawl4AI markdown and/or custom HTML extraction, tables, etc.).
     - Skip empty/minimal pages.
     - Write one `.md` file per URL under `depth_0/`, `depth_1/`, … and **only then** add that URL to `all_urls_by_depth` so counts match files.

6. **Checkpoint and errors**
   - Every N written pages we save a checkpoint (seen URLs, depth maps, errors, etc.).
   - On normal exit or on interrupt (e.g. SLURM time limit), we try to save a checkpoint so the next run can resume.
   - All failed URLs and timeouts go to `crawl_errors.json`.

7. **Finish**
   - Write `urls_by_depth.json` (from `all_urls_by_depth`), clear in-memory results to free RAM, and exit.

So in one sentence: we **load checkpoint → crawl seeds with BFS → enrich with manual link extraction and optional redirect checks → crawl additional URLs → for each result assign depth, extract content, write one file per URL and only then count it → checkpoint periodically and on exit.**

---

## Features (Summary)

- **Multi-depth BFS** — Depth 0 = seeds; 1, 2, … = configurable link levels.
- **Checkpoint/resume** — Resume after crash or SLURM time limit without re-crawling.
- **Accurate depth & counts** — Depth from merged maps; `urls_by_depth.json` counts only URLs that got a file.
- **Domain & exclusion** — Only `*.desy.de`; exclude specific subdomains; optional “don’t fetch if redirect goes to excluded”.
- **Filter chain** — Skip non-HTTP, binaries, calendars, and excluded hosts before queuing.
- **Tables & links** — Custom strategy keeps links and emails in tables as markdown.
- **PDF support** — Optional PDF text/image/table extraction when dependencies are available.
- **Anti-bot & headless** — Stealth and headless browser for stability.
- **Error & timeout logging** — All failures in JSON; timeouts flagged for retry.
- **SLURM** — Example script for cluster (CPUs, memory, time).

---

## Requirements

- Python 3.8+
- Dependencies: see `requirements.txt` (or use the **crawl4ai** mamba environment — Helmholtz colleagues can use the same `crawl4ai` mamba env for consistency).
- Network access to DESY sites; for long runs, a SLURM cluster (e.g. Maxwell at DESY) is recommended.

---

## Configuration (Main Knobs)

In `crawl_desy_all_urls.py`:

| What | Variable | Meaning |
|------|----------|--------|
| Start URLs | `ROOT_URLS` | Seed list (e.g. `https://desy.de/index_ger.html`, `index_eng.html`). |
| Output dir | `OUTPUT_DIR` | Where `depth_0/`, `depth_1/`, … and `.md` files go. |
| Log dir | `LOG_DIR` | Where `crawl_errors.json`, `urls_by_depth.json`, checkpoint live. |
| Resume | `USE_CHECKPOINT` | If `True`, load checkpoint and skip already-seen URLs. |
| Depth | `MAX_DEPTH` | Max link depth (0 = seeds only; 1, 2, … = follow links). |
| Concurrency | `CONCURRENT_TASKS` | Number of parallel browser tasks (e.g. 30). |
| Timeout | `PAGE_TIMEOUT` | Page load timeout in ms. |
| Excluded hosts | `EXCLUDED_DOMAINS` | Set of hosts never queued (e.g. `fater.desy.de`, `bib-pubdb1.desy.de`). |
| Redirect check | `CHECK_REDIRECTS_TO_EXCLUDED` | If `True`, resolve redirects before queuing and skip if final host is excluded. |
| Checkpoint frequency | `CHECKPOINT_FREQUENCY` | Save checkpoint every N **written** pages. |

---

## Usage

**On Maxwell cluster (DESY):** run the crawler from an interactive allocation so the browser has a display (or use headless). Steps:

1. Allocate a node and note the hostname:
   ```bash
   salloc -p maxgpu --time=10:00:00
   ```
2. SSH to the assigned node (e.g. `max-???` from the allocation message):
   ```bash
   ssh max-???
   ```
3. Activate the environment and run:
   ```bash
   . mamba-init
   mamba activate crawl4ai
   cd /path/to/crawl4ai
   python crawl_desy_all_urls.py
   ```

Alternatively, use the **crawl4ai** mamba env in your own path; Helmholtz colleagues can use the same env for reproducibility.

**Local run (outside cluster):**
```bash
pip install -r requirements.txt   # or: mamba env create -f environment.yml / use existing crawl4ai env
cd /path/to/crawl4ai
python crawl_desy_all_urls.py
```

**SLURM (long / background run):**
```bash
sbatch run_crawler.slurm
# Monitor: tail -f crawler_<jobid>.out
# Cancel: scancel <jobid>
```

Set `--cpus-per-task` in `run_crawler.slurm` to match `CONCURRENT_TASKS` (e.g. 30) so the job isn’t CPU-starved.

---

## Output Layout

```
desy_crawled/
├── depth_0/              # Seed URLs only
├── depth_1/               # One click from seeds
├── depth_2/               # Two clicks from seeds
├── ...
└── log/
    ├── crawl_checkpoint.json   # Resume state (seen URLs, depth maps, errors)
    ├── crawl_errors.json       # Failed URLs, timeouts, reasons
    └── urls_by_depth.json      # Per-depth counts and lists (matches written files)
```

Each `.md` file is named from the URL (e.g. `desy.de_index_ger.html.md`). Counts in `urls_by_depth.json` are **only** for URLs that received a file.

---

## Key Techniques in the Code (Where to Look)

| Technique | Where in code |
|-----------|----------------|
| Checkpoint load/save | `load_checkpoint()`, `save_checkpoint()`; start of `crawl_site()`; after every N writes; in `finally` on exit. |
| BFS strategy | `BFSDeepCrawlStrategy` (max_depth, filter_chain) passed into `CrawlerRunConfig`. |
| Filter chain | `exclusion_patterns` (regex list) → `RegexURLFilter` or `URLPatternFilter` → `FilterChain`. |
| Excluded domains | `EXCLUDED_DOMAINS`; used in link extraction and in exclusion_patterns for direct links. |
| Redirect resolution | `_resolve_redirect_final_host()` + cache; used before adding a link to the queue when `CHECK_REDIRECTS_TO_EXCLUDED` is True. |
| Depth assignment | After “Determine depth” comment: seed → `crawled_urls_with_depth_merged` → `additional_urls_with_depth_merged` → metadata → fallback 1 → cap. |
| Merge (current overwrites checkpoint) | Building `additional_urls_with_depth_merged` and `crawled_urls_with_depth_merged` from checkpoint then `.update(current_run)`. |
| Count only written URLs | `all_urls_by_depth[depth_str].append(url_entry)` is done **after** `filename.write_text(...)`, not before. |
| Custom tables | `LinkPreservingTableExtraction` class; used in `CrawlerRunConfig` as `table_extraction=...`. |
| main_content guard | Before `main_content.new_tag('p')`, use `tag_parent = main_content or soup` and check `hasattr(tag_parent, 'new_tag')`. |

---

## Error Handling & Logs

- **Timeouts** — Logged in `crawl_errors.json` with `is_timeout: true`; can retry later with higher `PAGE_TIMEOUT` if needed.
- **404 / empty** — Skipped (no file written, no count in `urls_by_depth`).
- **Extraction errors** — Caught per result; error appended to `all_errors`, checkpoint still saved so progress isn’t lost.

---

## Troubleshooting

- **Count in urls_by_depth.json ≠ number of files**  
  With the current code they should match (we append to `all_urls_by_depth` only after a successful write). If not, check for a different code version or custom changes.

- **Job killed by time limit**  
  Ensure checkpoint is saved on exit (see `finally` block). Resume with `USE_CHECKPOINT=True`; increase `--time` in SLURM if needed.

- **Too slow**  
  Increase `CONCURRENT_TASKS` and match `--cpus-per-task` in SLURM. Reduce `PAGE_TIMEOUT` only if pages are consistently fast.

- **Many “Invalid URL” or skip messages**  
  Filter chain and excluded domains are working; adjust `exclusion_patterns` or `EXCLUDED_DOMAINS` if you want to include more or fewer URLs.

---

## Questions

If you have any questions, you can reach me at: sara.taherimonfared@gmail.com

---

## License & Compliance

Use in line with DESY’s robots.txt and terms of service. The script is provided as-is.
