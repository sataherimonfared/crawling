import asyncio
from pathlib import Path
from crawl4ai import AsyncWebCrawler

ROOT_URL = "https://www.desy.de"
OUTPUT_DIR = Path("desy_crawled")
OUTPUT_DIR.mkdir(exist_ok=True)

CONCURRENT_TASKS = 3
MAX_DEPTH = 2

async def crawl_site():
    async with AsyncWebCrawler(
        max_tasks=CONCURRENT_TASKS,
        max_depth=MAX_DEPTH
    ) as crawler:
        # Crawl root URL and get results as a list
        results = await crawler.arun(ROOT_URL)
        # In 0.7.8, arun returns a single result if URL is one page
        # or a list if multiple pages (depends on configuration)
        if not isinstance(results, list):
            results = [results]

        for result in results:
            url_safe = result.url.replace("https://", "").replace("/", "_")
            filename = OUTPUT_DIR / f"{url_safe}.md"
            filename.write_text(result.markdown, encoding="utf-8")
            print(f"[SAVED] {result.url} → {filename}")

if __name__ == "__main__":
    asyncio.run(crawl_site())
