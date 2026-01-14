#!/usr/bin/env python3
"""
Compare original scraper vs pure crawl4ai on test URLs
Uses URLs from test_table_extraction.py
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

# Add paths
sys.path.insert(0, str(Path(__file__).parent.parent / "scraper-correcting-deepness-final"))
from improved_processing import ImprovedDESYContentProcessor
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from langchain_core.documents import Document

# Test URLs from test_table_extraction.py
TEST_URLS = [
    # Events page (should extract events)
    "https://www.desy.de/aktuelles/veranstaltungen/index_ger.html",
    # Member tables
    "https://atlas.desy.de/members/",
    "https://cms.desy.de/cms_members/",
    "https://pitz.desy.de/group_members/",
    "https://it.desy.de/about_us/gruppenleitung/management/index_eng.html",
    "https://astroparticle-physics.desy.de/about_us/group_members/theory/index_eng.html",
    "https://astroparticle-physics.desy.de/about_us/group_members/neutrino_astronomy/index_eng.html",
    "https://photon-science.desy.de/research/research_teams/magnetism_and_coherent_phenomena/group_members/index_eng.html",
    "https://photon-science.desy.de/facilities/petra_iii/beamlines/p23_in_situ_x_ray_diffraction_and_imaging/contact__staff/index_eng.html",
    # Publications page
    "https://astroparticle-physics.desy.de/research/neutrino_astronomy/publications/index_eng.html",
    # Researcher pages
    "https://www.desy.de/ueber_desy/leitende_wissenschaftler/christian_schwanenberger/index_ger.html",
    "https://ai.desy.de/people/heuser.html",
    # Contact page
    "https://www.desy.de/career/contact/index_eng.html",
]


async def process_with_original(url: str) -> Dict[str, Any]:
    """Process URL with original scraper"""
    processor = ImprovedDESYContentProcessor(max_depth=7, chunk_size=500, chunk_overlap=75)
    
    try:
        char_docs, struct_docs, full_docs = await processor.process_url(url, 0)
        
        # Categorize chunks
        table_chunks = [d for d in struct_docs if d.metadata.get("entity_type") == "table_row"]
        event_chunks = [d for d in struct_docs if d.metadata.get("entity_type") == "event"]
        researcher_chunks = [d for d in struct_docs if d.metadata.get("entity_type") == "researcher"]
        publication_chunks = [d for d in struct_docs if d.metadata.get("entity_type") == "publication"]
        structural_chunks = [d for d in struct_docs if d.metadata.get("chunk_type") == "structural"]
        
        # Extract MVR fields from table chunks
        mvr_fields = {}
        for chunk in table_chunks:
            meta = chunk.metadata
            if meta.get("mvr_name"):
                mvr_fields["has_mvr_name"] = True
            if meta.get("mvr_position"):
                mvr_fields["has_mvr_position"] = True
            if meta.get("mvr_phone"):
                mvr_fields["has_mvr_phone"] = True
            if meta.get("mvr_email"):
                mvr_fields["has_mvr_email"] = True
        
        return {
            "url": url,
            "status": "success",
            "total_chunks": len(char_docs) + len(struct_docs) + len(full_docs),
            "character_chunks": len(char_docs),
            "structural_chunks": len(structural_chunks),
            "table_chunks": len(table_chunks),
            "event_chunks": len(event_chunks),
            "researcher_chunks": len(researcher_chunks),
            "publication_chunks": len(publication_chunks),
            "full_text_chunks": len(full_docs),
            "mvr_fields_detected": mvr_fields,
            "sample_table_chunks": [
                {
                    "content_preview": chunk.page_content[:200],
                    "metadata": {k: v for k, v in chunk.metadata.items() if k.startswith("mvr_") or k in ["entity_type", "chunk_subtype"]}
                }
                for chunk in table_chunks[:3]
            ],
            "sample_event_chunks": [
                {
                    "content_preview": chunk.page_content[:200],
                    "metadata": {k: v for k, v in chunk.metadata.items() if k.startswith("event_") or k == "entity_type"}
                }
                for chunk in event_chunks[:3]
            ],
            "sample_researcher_chunks": [
                {
                    "content_preview": chunk.page_content[:200],
                    "metadata": {k: v for k, v in chunk.metadata.items() if k.startswith("researcher_") or k == "entity_type"}
                }
                for chunk in researcher_chunks[:3]
            ]
        }
    except Exception as e:
        return {
            "url": url,
            "status": "error",
            "error": str(e)
        }
    finally:
        await processor.close_session()


async def process_with_crawl4ai(url: str) -> Dict[str, Any]:
    """Process URL with pure crawl4ai"""
    async with AsyncWebCrawler() as crawler:
        try:
            result = await crawler.arun(url=url)
            
            if not result or not result.html:
                return {
                    "url": url,
                    "status": "error",
                    "error": "Failed to fetch"
                }
            
            soup = BeautifulSoup(result.html, 'html.parser')
            title = soup.title.text.strip() if soup.title else "No title"
            
            # Basic chunking from crawl4ai markdown
            chunks = []
            if result.markdown:
                chunks.append(Document(
                    page_content=result.markdown,
                    metadata={
                        "source": url,
                        "title": title,
                        "chunk_type": "structural",
                        "extraction_method": "crawl4ai_markdown"
                    }
                ))
            
            return {
                "url": url,
                "status": "success",
                "total_chunks": len(chunks),
                "structural_chunks": len(chunks),
                "table_chunks": 0,  # Not extracted yet
                "event_chunks": 0,  # Not extracted yet
                "researcher_chunks": 0,  # Not extracted yet
                "publication_chunks": 0,  # Not extracted yet
                "mvr_fields_detected": {},
                "crawl4ai_features": {
                    "markdown_extracted": bool(result.markdown),
                    "markdown_length": len(result.markdown) if result.markdown else 0,
                    "links_found": len(result.links) if hasattr(result, 'links') else 0,
                },
                "sample_chunks": [
                    {
                        "content_preview": chunk.page_content[:200],
                        "metadata": chunk.metadata
                    }
                    for chunk in chunks[:3]
                ]
            }
        except Exception as e:
            return {
                "url": url,
                "status": "error",
                "error": str(e)
            }


async def compare_urls(urls: List[str]) -> Dict[str, Any]:
    """Compare processing of URLs between original and crawl4ai"""
    results = {
        "timestamp": datetime.now().isoformat(),
        "urls_tested": len(urls),
        "comparisons": []
    }
    
    for url in urls:
        print(f"Processing: {url}")
        
        # Process with both
        original_result = await process_with_original(url)
        crawl4ai_result = await process_with_crawl4ai(url)
        
        # Compare
        comparison = {
            "url": url,
            "original": original_result,
            "crawl4ai": crawl4ai_result,
            "differences": {
                "total_chunks_diff": crawl4ai_result.get("total_chunks", 0) - original_result.get("total_chunks", 0),
                "table_chunks_missing": original_result.get("table_chunks", 0) - crawl4ai_result.get("table_chunks", 0),
                "event_chunks_missing": original_result.get("event_chunks", 0) - crawl4ai_result.get("event_chunks", 0),
                "researcher_chunks_missing": original_result.get("researcher_chunks", 0) - crawl4ai_result.get("researcher_chunks", 0),
                "publication_chunks_missing": original_result.get("publication_chunks", 0) - crawl4ai_result.get("publication_chunks", 0),
                "mvr_fields_missing": not bool(crawl4ai_result.get("mvr_fields_detected")) and bool(original_result.get("mvr_fields_detected")),
            }
        }
        
        results["comparisons"].append(comparison)
        
        # Print summary
        print(f"  Original: {original_result.get('total_chunks', 0)} chunks "
              f"(tables: {original_result.get('table_chunks', 0)}, "
              f"events: {original_result.get('event_chunks', 0)}, "
              f"researchers: {original_result.get('researcher_chunks', 0)})")
        print(f"  Crawl4AI: {crawl4ai_result.get('total_chunks', 0)} chunks "
              f"(tables: {crawl4ai_result.get('table_chunks', 0)}, "
              f"events: {crawl4ai_result.get('event_chunks', 0)}, "
              f"researchers: {crawl4ai_result.get('researcher_chunks', 0)})")
        print()
    
    return results


def print_summary(results: Dict[str, Any]):
    """Print comparison summary"""
    print("=" * 80)
    print("Comparison Summary")
    print("=" * 80)
    
    total_table_missing = sum(c["differences"]["table_chunks_missing"] for c in results["comparisons"])
    total_event_missing = sum(c["differences"]["event_chunks_missing"] for c in results["comparisons"])
    total_researcher_missing = sum(c["differences"]["researcher_chunks_missing"] for c in results["comparisons"])
    total_publication_missing = sum(c["differences"]["publication_chunks_missing"] for c in results["comparisons"])
    mvr_missing_count = sum(1 for c in results["comparisons"] if c["differences"]["mvr_fields_missing"])
    
    print(f"\nTotal URLs tested: {results['urls_tested']}")
    print(f"\nMissing Features in Crawl4AI:")
    print(f"  - Table chunks: {total_table_missing} missing across URLs")
    print(f"  - Event chunks: {total_event_missing} missing across URLs")
    print(f"  - Researcher chunks: {total_researcher_missing} missing across URLs")
    print(f"  - Publication chunks: {total_publication_missing} missing across URLs")
    print(f"  - MVR fields: {mvr_missing_count} URLs missing MVR extraction")
    
    print(f"\nURLs with Missing Features:")
    for comp in results["comparisons"]:
        diffs = comp["differences"]
        missing = []
        if diffs["table_chunks_missing"] > 0:
            missing.append(f"tables({diffs['table_chunks_missing']})")
        if diffs["event_chunks_missing"] > 0:
            missing.append(f"events({diffs['event_chunks_missing']})")
        if diffs["researcher_chunks_missing"] > 0:
            missing.append(f"researchers({diffs['researcher_chunks_missing']})")
        if diffs["publication_chunks_missing"] > 0:
            missing.append(f"publications({diffs['publication_chunks_missing']})")
        if diffs["mvr_fields_missing"]:
            missing.append("MVR_fields")
        
        if missing:
            print(f"  - {comp['url']}")
            print(f"    Missing: {', '.join(missing)}")


async def main():
    """Main function"""
    print("=" * 80)
    print("Comparing Original Scraper vs Pure Crawl4AI on Test URLs")
    print("=" * 80)
    print()
    
    results = await compare_urls(TEST_URLS)
    
    # Save results
    output_file = "test_urls_comparison.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nDetailed results saved to: {output_file}\n")
    
    # Print summary
    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())
