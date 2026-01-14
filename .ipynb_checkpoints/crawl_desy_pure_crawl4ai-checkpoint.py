#!/usr/bin/env python3
"""
Pure Crawl4AI implementation for DESY scraping
Uses ONLY crawl4ai features - no hybrid approach, no original scraper dependencies
"""

import asyncio
import argparse
import json
import logging
import signal
import sys
import time
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from bs4 import BeautifulSoup
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Optional: for language detection
try:
    from langdetect import detect
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawl4ai_pure.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PureCrawl4AIDESYScraper:
    """
    Pure Crawl4AI implementation for DESY scraping
    
    Uses ONLY crawl4ai features:
    - PruningContentFilter for content cleaning
    - Table extraction (via crawl4ai's default or DefaultTableExtraction if available)
    - fit_markdown for clean markdown
    - LangChain text splitters for chunking
    - Custom logic for specialized extraction (events, researchers)
    """
    
    def __init__(
        self,
        url_map_file: str,
        max_depth: int = 7,
        batch_size: int = 25,
        limit: int = 100,
        max_concurrent: int = 10,
        chunk_size: int = 500,
        chunk_overlap: int = 75,
        use_llm_extraction: bool = False
    ):
        self.url_map_file = url_map_file
        self.max_depth = max_depth
        self.batch_size = batch_size
        self.limit = limit
        self.max_concurrent = max_concurrent
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.use_llm_extraction = use_llm_extraction
        
        # Text splitter for chunking
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ". ", " ", ""]
        )
        
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
        self.checkpoint_file = "crawl4ai_pure_checkpoint.json"
        self.shutdown_flag = False
        self.processed_urls = set()
        self.error_urls = {}
        
        # Minimum chunk size
        self.MIN_CHUNK_CHARS = 50
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received, saving checkpoint...")
        self.shutdown_flag = True
    
    def _load_url_map(self) -> Dict[str, int]:
        """Load URL map from JSON file"""
        with open(self.url_map_file, 'r') as f:
            data = json.load(f)
        
        url_depth_map = {}
        
        # Handle structure with 'urls_by_depth' key (new format)
        if isinstance(data, dict) and 'urls_by_depth' in data:
            urls_by_depth = data['urls_by_depth']
            if isinstance(urls_by_depth, dict):
                for depth_str, urls in urls_by_depth.items():
                    try:
                        depth = int(depth_str)
                    except (ValueError, TypeError):
                        depth = 0
                    
                    if isinstance(urls, list):
                        for url in urls:
                            if isinstance(url, str) and url.strip():
                                # Ensure URL has protocol
                                url = url.strip()
                                if not url.startswith(('http://', 'https://', 'file://', 'raw:')):
                                    # Try to add https:// if it looks like a URL
                                    if url.startswith('www.') or '.' in url:
                                        url = 'https://' + url
                                    else:
                                        # Skip invalid URLs
                                        continue
                                url_depth_map[url] = depth
                    elif isinstance(urls, dict):
                        # If urls is a dict, use keys as URLs
                        for url in urls.keys():
                            if isinstance(url, str) and url.strip():
                                url = url.strip()
                                if not url.startswith(('http://', 'https://', 'file://', 'raw:')):
                                    if url.startswith('www.') or '.' in url:
                                        url = 'https://' + url
                                    else:
                                        continue
                                url_depth_map[url] = depth
        
        # Handle simple dict format (url -> depth)
        elif isinstance(data, dict):
            for url, depth_str in data.items():
                # Skip metadata keys
                if url in ['total_urls', 'total_depth_levels', 'urls_by_depth', 'missing_urls', 
                           'error_urls', 'error_categories', 'personal_pages', 'personal_pages_count',
                           'personal_pages_in_visited_count', 'personal_pages_not_in_visited',
                           'personal_pages_by_depth', 'page_character_counts', 'domain_stats', 'crawl_stats']:
                    continue
                
                if isinstance(url, str) and url.strip():
                    try:
                        depth = int(depth_str)
                    except (ValueError, TypeError):
                        depth = 0
                    
                    url = url.strip()
                    if not url.startswith(('http://', 'https://', 'file://', 'raw:')):
                        if url.startswith('www.') or '.' in url:
                            url = 'https://' + url
                        else:
                            continue
                    url_depth_map[url] = depth
        
        # Handle list format
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    url = item.get('url', '')
                    depth = item.get('depth', 0)
                    if url and isinstance(url, str) and url.strip():
                        url = url.strip()
                        if not url.startswith(('http://', 'https://', 'file://', 'raw:')):
                            if url.startswith('www.') or '.' in url:
                                url = 'https://' + url
                            else:
                                continue
                        url_depth_map[url] = depth
        
        logger.info(f"Loaded {len(url_depth_map)} URLs from URL map")
        return url_depth_map
    
    def detect_language(self, soup: BeautifulSoup, text_sample: str, url: str) -> str:
        """Detect language using HTML attributes, URL pattern, and content"""
        # Check URL pattern
        if url and url.lower().endswith('_ger.html'):
            return 'de'
        if url and url.lower().endswith('_eng.html'):
            return 'en'
        
        # Check HTML lang attribute
        if soup and soup.html:
            html_lang = soup.html.get('lang') or soup.html.get('xml:lang')
            if html_lang:
                lang_code = html_lang.strip().lower().split('-')[0]
                if len(lang_code) == 2:
                    return lang_code
        
        # Check meta tags
        meta_lang = soup.find('meta', attrs={'http-equiv': 'content-language'})
        if meta_lang and meta_lang.get('content'):
            lang_code = meta_lang.get('content').strip().lower().split('-')[0]
            if len(lang_code) == 2:
                return lang_code
        
        # Use langdetect if available
        if LANGDETECT_AVAILABLE and text_sample and len(text_sample) >= 50:
            try:
                return detect(text_sample[:1000])
            except Exception:
                pass
        
        return 'en'  # Default
    
    def extract_links(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract and categorize links from page"""
        all_links_data = []
        personal_websites = []
        publications = []
        projects = []
        
        # Navigation/footer patterns to exclude
        nav_footer_patterns = [
            r'nav', r'menu', r'footer', r'breadcrumb', r'sidebar',
            r'cookie', r'privacy', r'imprint', r'disclaimer',
            r'print', r'language', r'lang', r'change language',
            r'phonebook', r'pbook', r'index_print', r'#', r'javascript:',
            r'desy\.de/research', r'desy\.de/news', r'desy\.de/events',
            r'^/$', r'^#', r'index_eng\.html', r'index_ger\.html'
        ]
        nav_footer_regex = re.compile('|'.join(nav_footer_patterns), re.I)
        
        # Keywords for categorization
        homepage_keywords = ['homepage', 'website', 'personal', 'persönlich', 'persönliche homepage']
        publication_keywords = ['publication', 'paper', 'article', 'preprint', 'arxiv', 'doi', 'journal', 'publikation']
        project_keywords = ['project', 'research', 'experiment', 'study', 'work', 'projekt']
        
        base_domain = urlparse(url).netloc
        
        try:
            all_links = soup.find_all('a', href=True, limit=200)
        except Exception:
            all_links = []
        
        for link in all_links:
            try:
                href = link.get('href', '').strip()
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                
                # Skip navigation/footer links
                if nav_footer_regex.search(href):
                    continue
                
                # Resolve relative URLs
                full_url = urljoin(url, href)
                parsed = urlparse(full_url)
                
                # Skip if same domain (internal link)
                is_external = parsed.netloc and parsed.netloc != base_domain
                
                link_text = link.get_text(strip=True).lower()
                
                # Categorize
                if any(kw in link_text for kw in homepage_keywords):
                    personal_websites.append({'url': full_url, 'text': link_text})
                elif any(kw in link_text for kw in publication_keywords):
                    publications.append({'url': full_url, 'text': link_text})
                elif any(kw in link_text for kw in project_keywords):
                    projects.append({'url': full_url, 'text': link_text})
                
                all_links_data.append({'url': full_url, 'text': link_text, 'external': is_external})
            except Exception:
                continue
        
        return {
            'total_count': len(all_links_data),
            'personal_websites': personal_websites,
            'publications': publications,
            'projects': projects,
            'all_links': all_links_data
        }
    
    def is_event_table(self, table_data: Dict) -> bool:
        """Detect if a table is an event table"""
        # Check for event-related keywords in headers or data
        event_keywords = ['date', 'time', 'speaker', 'title', 'place', 'location', 'event', 'veranstaltung']
        
        # Check headers
        headers = table_data.get('headers', [])
        if headers:
            header_text = ' '.join(str(h) for h in headers).lower()
            if any(kw in header_text for kw in event_keywords):
                return True
        
        # Check first few rows
        rows = table_data.get('rows', []) or table_data.get('data', [])
        if rows and len(rows) > 0:
            first_row_text = ' '.join(str(cell) for cell in rows[0] if cell).lower()
            if any(kw in first_row_text for kw in event_keywords):
                return True
        
        return False
    
    def extract_event_from_table_row(self, row: List, headers: List, url: str) -> Optional[Dict]:
        """Extract event information from a table row"""
        if not row or not headers:
            return None
        
        # Map headers to event fields
        event = {}
        for i, header in enumerate(headers):
            header_lower = str(header).lower()
            if i < len(row):
                cell_value = str(row[i]).strip()
                if not cell_value:
                    continue
                
                if any(kw in header_lower for kw in ['date', 'time', 'datum', 'zeit']):
                    event['date'] = cell_value
                elif any(kw in header_lower for kw in ['place', 'location', 'ort', 'raum']):
                    event['place'] = cell_value
                elif any(kw in header_lower for kw in ['type', 'typ', 'category']):
                    event['type'] = cell_value
                elif any(kw in header_lower for kw in ['title', 'titel', 'topic']):
                    event['title'] = cell_value
                elif any(kw in header_lower for kw in ['speaker', 'sprecher', 'lecturer']):
                    event['speaker'] = cell_value
                elif any(kw in header_lower for kw in ['affiliation', 'institution', 'group']):
                    event['affiliation'] = cell_value
                elif any(kw in header_lower for kw in ['link', 'url']):
                    event['link'] = cell_value
        
        # Only return if we have at least title or date
        if event.get('title') or event.get('date'):
            return event
        return None
    
    def is_researcher_page(self, soup: BeautifulSoup, url: str) -> bool:
        """Detect if page is a researcher profile page"""
        # Check URL pattern
        researcher_patterns = [
            r'/people/', r'/person/', r'/staff/', r'/member/',
            r'/scientist/', r'/researcher/', r'/leitende_wissenschaftler/',
            r'/group_members/', r'/members/'
        ]
        if any(re.search(pattern, url, re.I) for pattern in researcher_patterns):
            return True
        
        # Check page content for researcher indicators
        text = soup.get_text().lower() if soup else ""
        researcher_keywords = ['ph.d', 'professor', 'dr.', 'researcher', 'scientist', 'group leader']
        if any(kw in text for kw in researcher_keywords):
            return True
        
        return False
    
    def extract_researcher_info(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract researcher information from page"""
        info = {}
        
        # Extract name from title or h1
        title = soup.title.text.strip() if soup.title else ""
        h1 = soup.find('h1')
        if h1:
            name = h1.get_text(strip=True)
            if name and len(name) < 100:  # Reasonable name length
                info['name'] = name
        elif title:
            # Try to extract name from title
            name_match = re.search(r'^([^|–-]+)', title)
            if name_match:
                info['name'] = name_match.group(1).strip()
        
        # Extract contact info
        text = soup.get_text()
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        if email_match:
            info['email'] = email_match.group(0)
        
        phone_match = re.search(r'\+?\d[\d\s\-\(\)]{7,}\d', text)
        if phone_match:
            info['phone'] = phone_match.group(0)
        
        return info
    
    def _post_process_markdown(self, markdown: str) -> str:
        """Post-process markdown to remove remaining noise"""
        if not markdown:
            return ""
        
        lines = markdown.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Skip very short lines (likely navigation items)
            if len(line) < 20:
                # Check if it's just a URL or navigation keyword
                if line.startswith('http') or any(kw in line.lower() for kw in ['nav', 'menu', 'cookie', 'privacy']):
                    continue
            
            # Remove standalone URLs (keep URLs that are part of sentences)
            # This regex removes URLs that are on their own line
            if re.match(r'^https?://[^\s]+$', line):
                continue
            
            # Remove lines that are only navigation keywords
            nav_only_patterns = [
                r'^(navigation|menu|footer|header|breadcrumb|cookie|privacy|impressum|datenschutz)$',
                r'^(Navigation|Menu|Footer|Header|Breadcrumb)$'
            ]
            if any(re.match(pattern, line, re.I) for pattern in nav_only_patterns):
                continue
            
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def _is_navigation_chunk(self, text: str) -> bool:
        """Check if a chunk is primarily navigation content"""
        text_lower = text.lower()
        
        # Navigation keywords
        nav_keywords = ['navigation', 'menu', 'breadcrumb', 'footer', 'header', 'sidebar']
        nav_count = sum(1 for kw in nav_keywords if kw in text_lower)
        
        # If more than 30% of words are navigation-related, it's likely navigation
        words = text_lower.split()
        if len(words) > 0:
            nav_ratio = nav_count / len(words)
            if nav_ratio > 0.3:
                return True
        
        # Check if it's mostly URLs
        url_count = len(re.findall(r'https?://[^\s]+', text))
        if url_count > 0 and len(text.split()) < 10:
            return True
        
        return False
    
    async def process_single_url(
        self,
        url: str,
        depth: int,
        crawler: AsyncWebCrawler
    ) -> Dict[str, Any]:
        """Process a single URL using pure crawl4ai with enhanced content filtering"""
        try:
            # Enhanced PruningContentFilter with more aggressive settings
            prune_filter = PruningContentFilter(
                threshold=0.60,  # Increased from 0.48 for more aggressive pruning
                threshold_type="dynamic",  # Changed from "fixed" for adaptive filtering
                min_word_threshold=8  # Increased from 0 to filter short snippets
            )
            
            markdown_generator = DefaultMarkdownGenerator(
                content_filter=prune_filter
            )
            
            # Comprehensive element exclusion using CrawlerRunConfig parameters
            # This removes navigation, footers, headers, and boilerplate BEFORE markdown generation
            excluded_tags = [
                'nav', 'footer', 'header', 'aside', 
                'script', 'style', 'noscript',
                'iframe', 'embed', 'object'
            ]
            
            # DESY-specific CSS selectors for navigation and boilerplate
            excluded_selector = (
                '.navigation, .menu, .breadcrumb, .cookie, '
                '.privacy, .impressum, .datenschutz, .accessibility, '
                '.barrierefreiheit, .footer, .header, .sidebar, '
                '#footer, #header, #sidebar, #nav, #menu, '
                '[class*="nav"], [class*="menu"], [class*="footer"], '
                '[class*="header"], [class*="cookie"], [class*="privacy"], '
                '[id*="nav"], [id*="menu"], [id*="footer"], [id*="header"], '
                '[class*="sprungnavigation"], [class*="breadcrumb"], '
                '[id*="sprungnavigation"], [id*="breadcrumb"]'
            )
            
            # Table extraction is enabled by default in CrawlerRunConfig
            # Try to use DefaultTableExtraction if available, otherwise use default behavior
            try:
                from crawl4ai import DefaultTableExtraction
                table_extraction = DefaultTableExtraction()
                run_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    markdown_generator=markdown_generator,
                    table_extraction=table_extraction,
                    excluded_tags=excluded_tags,
                    excluded_selector=excluded_selector,
                    word_count_threshold=10,  # Filter out text blocks with < 10 words
                    remove_forms=True  # Remove cookie consent forms and other forms
                )
            except (ImportError, AttributeError):
                # Table extraction will use default behavior
                # Some versions of crawl4ai enable table extraction by default
                run_config = CrawlerRunConfig(
                    cache_mode=CacheMode.ENABLED,
                    markdown_generator=markdown_generator,
                    excluded_tags=excluded_tags,
                    excluded_selector=excluded_selector,
                    word_count_threshold=10,
                    remove_forms=True
                )
            
            # Fetch with crawl4ai
            result = await crawler.arun(url=url, config=run_config)
            
            if not result or not result.html:
                return {
                    'url': url,
                    'depth': depth,
                    'error': 'ProcessingError: Failed to fetch or invalid page'
                }
            
            # Parse HTML
            soup = BeautifulSoup(result.html, 'html.parser')
            title = soup.title.text.strip() if soup.title else "No title"
            
            # Detect language
            text_sample = soup.get_text()[:1000] if soup else ""
            language = self.detect_language(soup, text_sample, url)
            
            # Extract links
            links_data = self.extract_links(soup, url)
            
            # Get cleaned markdown
            if hasattr(result.markdown, 'fit_markdown'):
                cleaned_markdown = result.markdown.fit_markdown or ""
                raw_markdown = result.markdown.raw_markdown or ""
            else:
                cleaned_markdown = result.markdown or ""
                raw_markdown = cleaned_markdown
            
            # Post-process markdown to remove remaining noise
            cleaned_markdown = self._post_process_markdown(cleaned_markdown)
            
            # Create chunks from cleaned markdown
            all_chunks = []
            
            # Structural chunks from fit_markdown
            if cleaned_markdown and cleaned_markdown.strip():
                text_chunks = self.text_splitter.split_text(cleaned_markdown)
                for i, chunk_text in enumerate(text_chunks):
                    # Additional filtering: skip very short chunks and navigation-only chunks
                    if len(chunk_text.strip()) >= self.MIN_CHUNK_CHARS and not self._is_navigation_chunk(chunk_text):
                        chunk_metadata = {
                            "source": url,
                            "title": title,
                            "depth": depth,
                            "language": language,
                            "chunk_type": "structural",
                            "chunk_index": i,
                            "total_chunks": len(text_chunks),
                            "extraction_method": "crawl4ai_pure_fit_markdown",
                            "page_link_count": links_data.get('total_count', 0),
                            "has_personal_websites": len(links_data.get('personal_websites', [])) > 0,
                            "has_publications": len(links_data.get('publications', [])) > 0,
                            "has_projects": len(links_data.get('projects', [])) > 0,
                            "personal_websites_count": len(links_data.get('personal_websites', [])),
                            "publications_count": len(links_data.get('publications', [])),
                            "projects_count": len(links_data.get('projects', []))
                        }
                        all_chunks.append(Document(
                            page_content=chunk_text,
                            metadata=chunk_metadata
                        ))
            
            # Process tables
            if hasattr(result, 'tables') and result.tables:
                for table_idx, table in enumerate(result.tables):
                    # Handle different table formats
                    if isinstance(table, dict):
                        headers = table.get('headers', [])
                        rows = table.get('rows', []) or table.get('data', [])
                        
                        # Check if event table
                        is_event = self.is_event_table(table)
                        
                        for row_idx, row in enumerate(rows):
                            if is_event:
                                # Extract event
                                event = self.extract_event_from_table_row(row, headers, url)
                                if event:
                                    event_content = f"Type: {event.get('type', 'N/A')}\n"
                                    event_content += f"Title: {event.get('title', 'N/A')}\n"
                                    event_content += f"Date/Time: {event.get('date', 'N/A')}\n"
                                    event_content += f"Place: {event.get('place', 'N/A')}\n"
                                    event_content += f"Speaker: {event.get('speaker', 'N/A')}\n"
                                    if event.get('affiliation'):
                                        event_content += f"Affiliation: {event.get('affiliation')}\n"
                                    if event.get('link'):
                                        event_content += f"Link: {event.get('link')}\n"
                                    
                                    event_metadata = {
                                        "source": url,
                                        "title": title,
                                        "depth": depth,
                                        "language": language,
                                        "chunk_type": "entity_centric",
                                        "entity_type": "event",
                                        "table_index": table_idx,
                                        "row_index": row_idx,
                                        "event_date": event.get('date'),
                                        "event_place": event.get('place'),
                                        "event_type": event.get('type'),
                                        "event_title": event.get('title'),
                                        "event_speaker": event.get('speaker'),
                                        "event_affiliation": event.get('affiliation'),
                                        "event_link": event.get('link'),
                                        "extraction_method": "crawl4ai_pure_table_extraction",
                                        "page_link_count": links_data.get('total_count', 0)
                                    }
                                    all_chunks.append(Document(
                                        page_content=event_content,
                                        metadata=event_metadata
                                    ))
                            else:
                                # Regular table row
                                row_content = " | ".join(str(cell) for cell in row if cell)
                                if row_content.strip():
                                    row_metadata = {
                                        "source": url,
                                        "title": title,
                                        "depth": depth,
                                        "language": language,
                                        "chunk_type": "entity_centric",
                                        "entity_type": "table_row",
                                        "table_index": table_idx,
                                        "row_index": row_idx,
                                        "extraction_method": "crawl4ai_pure_table_extraction",
                                        "page_link_count": links_data.get('total_count', 0)
                                    }
                                    all_chunks.append(Document(
                                        page_content=row_content,
                                        metadata=row_metadata
                                    ))
            
            # Check for researcher page
            if self.is_researcher_page(soup, url):
                researcher_info = self.extract_researcher_info(soup, url)
                if researcher_info.get('name'):
                    researcher_content = f"Name: {researcher_info.get('name')}\n"
                    if researcher_info.get('email'):
                        researcher_content += f"Email: {researcher_info.get('email')}\n"
                    if researcher_info.get('phone'):
                        researcher_content += f"Phone: {researcher_info.get('phone')}\n"
                    
                    researcher_metadata = {
                        "source": url,
                        "title": title,
                        "depth": depth,
                        "language": language,
                        "chunk_type": "entity_centric",
                        "entity_type": "researcher",
                        "researcher_name": researcher_info.get('name'),
                        "researcher_email": researcher_info.get('email'),
                        "researcher_phone": researcher_info.get('phone'),
                        "extraction_method": "crawl4ai_pure_researcher_extraction",
                        "page_link_count": links_data.get('total_count', 0),
                        "has_personal_websites": len(links_data.get('personal_websites', [])) > 0
                    }
                    all_chunks.append(Document(
                        page_content=researcher_content,
                        metadata=researcher_metadata
                    ))
            
            # Character chunks from raw markdown (if different from cleaned)
            if raw_markdown and raw_markdown != cleaned_markdown and raw_markdown.strip():
                char_chunks = self.text_splitter.split_text(raw_markdown)
                for i, chunk_text in enumerate(char_chunks):
                    if len(chunk_text.strip()) >= self.MIN_CHUNK_CHARS:
                        char_metadata = {
                            "source": url,
                            "title": title,
                            "depth": depth,
                            "language": language,
                            "chunk_type": "character",
                            "chunk_index": i,
                            "total_chunks": len(char_chunks),
                            "extraction_method": "crawl4ai_pure_raw_markdown"
                        }
                        all_chunks.append(Document(
                            page_content=chunk_text,
                            metadata=char_metadata
                        ))
            
            # Categorize chunks
            char_docs = [doc for doc in all_chunks if doc.metadata.get("chunk_type") == "character"]
            struct_docs = [doc for doc in all_chunks if doc.metadata.get("chunk_type") in ("structural", "entity_centric")]
            full_docs = [doc for doc in all_chunks if doc.metadata.get("chunk_type") == "full_text"]
            
            total_chunks = len(all_chunks)
            
            if total_chunks > 0:
                return {
                    'url': url,
                    'depth': depth,
                    'chunks': total_chunks,
                    'char_chunks': len(char_docs),
                    'struct_chunks': len(struct_docs),
                    'full_chunks': len(full_docs),
                    'character_chunks': {
                        'text_chunks': [doc.page_content for doc in char_docs],
                        'document_metadata': [doc.metadata for doc in char_docs]
                    },
                    'structural_chunks': {
                        'text_chunks': [doc.page_content for doc in struct_docs],
                        'document_metadata': [doc.metadata for doc in struct_docs]
                    },
                    'full_text_chunks': {
                        'text_chunks': [doc.page_content for doc in full_docs],
                        'document_metadata': [doc.metadata for doc in full_docs]
                    },
                    'crawl4ai_features': {
                        'html_length': len(result.html),
                        'cleaned_markdown_length': len(cleaned_markdown),
                        'raw_markdown_length': len(raw_markdown),
                        'links_found': links_data.get('total_count', 0),
                        'tables_found': len(result.tables) if hasattr(result, 'tables') and result.tables else 0,
                        'personal_websites': len(links_data.get('personal_websites', [])),
                        'publications': len(links_data.get('publications', [])),
                        'projects': len(links_data.get('projects', [])),
                        'language_detected': language,
                        'processing_method': 'crawl4ai_pure',
                        'uses_pruning_content_filter': True,
                        'uses_default_table_extraction': True,
                        'uses_fit_markdown': True
                    }
                }
            else:
                return {
                    'url': url,
                    'depth': depth,
                    'error': 'ProcessingError: No chunks extracted'
                }
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.warning(f"Error processing {url}: {error_type}: {error_msg}")
            return {
                'url': url,
                'depth': depth,
                'error': f"{error_type}: {error_msg}"
            }
    
    async def process_batch(self, urls: List[Tuple[str, int]]) -> List[Dict[str, Any]]:
        """Process a batch of URLs concurrently"""
        async with AsyncWebCrawler() as crawler:
            tasks = [self.process_single_url(url, depth, crawler) for url, depth in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Task failed with exception: {result}")
                    processed_results.append({
                        'url': 'unknown',
                        'depth': 0,
                        'error': f"Exception: {str(result)}"
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
    
    async def run(self):
        """Main execution loop"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        url_depth_map = self._load_url_map()
        urls_to_process = [(url, depth) for url, depth in url_depth_map.items() 
                          if depth <= self.max_depth and url not in self.processed_urls]
        
        if self.limit > 0:
            urls_to_process = urls_to_process[:self.limit]
        
        logger.info(f"Starting pure crawl4ai scraper: {len(urls_to_process)} URLs to process")
        
        for i in range(0, len(urls_to_process), self.batch_size):
            if self.shutdown_flag:
                logger.info("Shutdown flag set, stopping...")
                break
            
            batch = urls_to_process[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}: {len(batch)} URLs")
            
            results = await self.process_batch(batch)
            
            for result in results:
                self.processed_count += 1
                if 'error' in result:
                    self.error_count += 1
                    self.error_urls[result['url']] = result['error']
                else:
                    self.processed_urls.add(result['url'])
            
            # Save checkpoint
            self._save_checkpoint()
            
            logger.info(f"Processed: {self.processed_count}/{len(urls_to_process)}, "
                       f"Errors: {self.error_count}, "
                       f"Time: {time.time() - self.start_time:.1f}s")
        
        logger.info("Pure crawl4ai scraper completed")
    
    def _save_checkpoint(self):
        """Save checkpoint"""
        checkpoint = {
            'processed_urls': list(self.processed_urls),
            'error_urls': self.error_urls,
            'processed_count': self.processed_count,
            'error_count': self.error_count
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Pure Crawl4AI DESY Scraper')
    parser.add_argument('--url-map', required=True, help='URL map JSON file')
    parser.add_argument('--max-depth', type=int, default=7, help='Maximum depth')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size')
    parser.add_argument('--limit', type=int, default=100, help='Limit number of URLs')
    parser.add_argument('--max-concurrent', type=int, default=10, help='Max concurrent requests')
    parser.add_argument('--chunk-size', type=int, default=500, help='Chunk size')
    parser.add_argument('--chunk-overlap', type=int, default=75, help='Chunk overlap')
    parser.add_argument('--use-llm', action='store_true', help='Use LLM extraction (paid)')
    
    args = parser.parse_args()
    
    scraper = PureCrawl4AIDESYScraper(
        url_map_file=args.url_map,
        max_depth=args.max_depth,
        batch_size=args.batch_size,
        limit=args.limit,
        max_concurrent=args.max_concurrent,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        use_llm_extraction=args.use_llm
    )
    
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
