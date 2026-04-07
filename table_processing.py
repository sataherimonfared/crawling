"""
Table Processing Module (extracted in refactoring Step 3)

All table extraction, enrichment, formatting, and link-injection functions.
This module is imported by the main crawler as `_table_processing`.
"""

import re
import logging
from html import unescape

# BeautifulSoup for HTML parsing
try:
    from bs4 import BeautifulSoup, NavigableString
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False

# Table extraction support
try:
    from crawl4ai import DefaultTableExtraction
    from crawl4ai import TableExtractionStrategy
    TABLE_EXTRACTION_AVAILABLE = True
except ImportError:
    TABLE_EXTRACTION_AVAILABLE = False
    TableExtractionStrategy = None
    DefaultTableExtraction = None


# ============================================================================
# Helper utilities (used by table functions)
# ============================================================================

def _is_empty_or_whitespace(text):
    """Check if text is None, empty, or contains only whitespace."""
    return not text or not text.strip()


def _is_in_navigation(elem):
    """Check if a BeautifulSoup element is inside navigation/header/footer/aside containers."""
    if not elem:
        return False
    return elem.find_parent(['nav', 'header', 'footer', 'aside']) is not None


# ============================================================================
# PUBDB helpers (used by extract_headings_and_tables_in_dom_order)
# ============================================================================

def is_pubdb_url(url):
    """
    Check if a URL is a PUBDB (bib-pubdb1.desy.de) page.
    These pages require special filtering to remove navigation/search UI elements.
    """
    if not url:
        return False
    url_lower = url.lower()
    return 'bib-pubdb1.desy.de' in url_lower or 'bib-pubdb' in url_lower


def is_pubdb_content(html_content):
    """
    Check if HTML content is from a PUBDB page by detecting PUBDB-specific markers.
    This handles cases where pages redirect to or embed PUBDB content.
    """
    if not html_content:
        return False
    html_lower = str(html_content).lower()
    # Check for PUBDB domain in links/content
    pubdb_indicators = [
        'bib-pubdb1.desy.de',
        'bib-pubdb',
        'guest :: login',
        'search: | [search tips]',
        'sort by: | display results:',
        'results overview',
        'interested in being notified about new results'
    ]
    # Need at least 2 indicators to be confident it's PUBDB content
    matches = sum(1 for indicator in pubdb_indicators if indicator in html_lower)
    return matches >= 2


# PUBDB UI keywords that indicate navigation/search interface (not publication records)
_PUBDB_UI_KEYWORDS = [
    'guest', 'login', 'search:', 'sort by:', 'display results:',
    'output format:', 'search tips', 'collections:', 'name | info',
    'results overview', 'try your search', 'rss feed', 'interested in being notified',
    'haven\'t found what you were looking for'
]



def is_pubdb_ui_table(table_text):
    """
    Check if a table is a PUBDB UI table (navigation/search interface) rather than publication records.
    
    Args:
        table_text: The text content of the table (lowercase recommended)
    
    Returns:
        bool: True if the table is a UI table, False if it contains publication records
    """
    if not table_text:
        return False
    
    # Ensure lowercase for consistent matching
    table_text_lower = table_text.lower() if not isinstance(table_text, str) or table_text != table_text.lower() else table_text
    
    # Check if this table contains publication records (PUBDB-YYYY-NNNNN pattern)
    has_publication_id = bool(re.search(r'pubdb-\d{4}-\d{5}', table_text_lower, re.I))
    
    # Only filter if it has UI keywords AND doesn't contain publication IDs
    # Also filter if it contains "pubdb" in UI context (login/pubdb link, not PUBDB-ID)
    has_ui_keywords = any(keyword in table_text_lower for keyword in _PUBDB_UI_KEYWORDS)
    has_pubdb_ui_context = ('pubdb' in table_text_lower and 
                          ('login' in table_text_lower or 'guest' in table_text_lower or 
                           'search:' in table_text_lower or 'submit' in table_text_lower))
    
    return (has_ui_keywords or has_pubdb_ui_context) and not has_publication_id


def _is_pubdb_page(url, html_content):
    """
    Check if a page is a PUBDB page by checking both URL and content.
    This handles cases where pages redirect to or embed PUBDB content.
    
    Args:
        url: The page URL (can be None)
        html_content: The HTML content (can be None)
    
    Returns:
        bool: True if the page is a PUBDB page
    """
    return (url and is_pubdb_url(url)) or is_pubdb_content(html_content)




# ============================================================================
# Custom Table Extraction Strategy that Preserves Links
# ============================================================================

# Guard: only define class when crawl4ai is available (TableExtractionStrategy != None)
_BaseStrategy = TableExtractionStrategy if TableExtractionStrategy is not None else object

class LinkPreservingTableExtraction(_BaseStrategy):
    """
    Custom table extraction strategy that preserves HTML links and emails in table cells.
    
    This strategy wraps DefaultTableExtraction but post-processes the extracted tables
    to convert HTML links to markdown format, ensuring emails and URLs are preserved.
    """
    
    def __init__(self, table_score_threshold=3, min_rows=1, min_cols=2, verbose=True):
        """
        Initialize the link-preserving table extraction strategy.
        
        Args:
            table_score_threshold: Minimum score for a table to be extracted (lower = more tables)
            min_rows: Minimum number of rows for a valid table
            min_cols: Minimum number of columns for a valid table
            verbose: Enable verbose logging
        """
        self.base_strategy = DefaultTableExtraction(
            table_score_threshold=table_score_threshold,
            min_rows=min_rows,
            min_cols=min_cols,
            verbose=verbose
        )
        self.verbose = verbose
        # Add logger attribute that TableExtractionStrategy expects
        self.logger = logging.getLogger(__name__)
    
    def extract_tables(self, element, **kwargs):
        """
        Extract tables from HTML, preserving links in cells.
        
        Args:
            element: HTML element (can be string, BeautifulSoup, or element)
            **kwargs: Additional parameters (may include url, extraction_strategy, etc.)
            
        Returns:
            List of extracted tables with links preserved as markdown
        """
        # Extract URL from kwargs if available
        url = kwargs.get('url', None)
        
        # Convert element to HTML string if needed
        if isinstance(element, str):
            html = element
        else:
            # If it's a BeautifulSoup object or other element, convert to string
            html = str(element) if hasattr(element, '__str__') else str(element)
        
        # First, use DefaultTableExtraction to get the table structure
        # Pass element and kwargs to match expected signature
        if hasattr(self.base_strategy, 'extract_tables'):
            tables = self.base_strategy.extract_tables(element, **kwargs)
        else:
            # Fallback for older versions that might use extract()
            tables = self.base_strategy.extract(html, url) if hasattr(self.base_strategy, 'extract') else []
        
        if not tables or not BEAUTIFULSOUP_AVAILABLE:
            return tables
        
        # Parse HTML to extract link information
        try:
            soup = BeautifulSoup(html, 'lxml')
            html_tables = soup.find_all('table')
            
            # Process each extracted table
            for table_idx, table in enumerate(tables):
                if table_idx >= len(html_tables):
                    continue
                
                html_table = html_tables[table_idx]
                
                # Process headers
                if 'headers' in table:
                    table['headers'] = self._process_row(
                        table['headers'],
                        html_table,
                        is_header=True
                    )
                
                # Process rows
                if 'rows' in table:
                    processed_rows = []
                    for row_idx, row in enumerate(table['rows']):
                        processed_row = self._process_row(
                            row,
                            html_table,
                            row_index=row_idx
                        )
                        processed_rows.append(processed_row)
                    table['rows'] = processed_rows
        
        except Exception as e:
            if self.verbose:
                print(f"[WARNING] Failed to preserve links in tables: {e}")
        
        return tables
    
    def _process_row(self, row_data, html_table, is_header=False, row_index=0):
        """
        Process a table row, converting HTML links to markdown.
        
        Args:
            row_data: List of cell values (plain text)
            html_table: BeautifulSoup table element
            is_header: Whether this is a header row
            row_index: Index of the row in the table
            
        Returns:
            List of processed cell values with links as markdown
        """
        processed_cells = []
        
        try:
            # Find the corresponding row in HTML - use recursive=True to catch all rows
            rows = html_table.find_all('tr', recursive=True)
            # Filter to ensure rows belong to this table, not nested tables
            rows = [r for r in rows if r.find_parent('table') == html_table]
            
            # Determine which HTML row to use
            html_row_idx = row_index
            if is_header:
                # Check if there's a thead
                thead = html_table.find('thead')
                if thead:
                    header_rows = thead.find_all('tr', recursive=True)
                    header_rows = [r for r in header_rows if r.find_parent('table') == html_table]
                    if row_index < len(header_rows):
                        html_row = header_rows[row_index]
                    else:
                        html_row = None
                else:
                    # First row might be header
                    html_row = rows[0] if rows else None
            else:
                # Data row - skip header rows
                tbody = html_table.find('tbody')
                if tbody:
                    tbody_rows = tbody.find_all('tr', recursive=True)
                    tbody_rows = [r for r in tbody_rows if r.find_parent('table') == html_table]
                    if row_index < len(tbody_rows):
                        html_row = tbody_rows[row_index]
                    else:
                        html_row = None
                else:
                    # No tbody, skip first row if it's a header
                    start_idx = 1 if html_table.find('th') else 0
                    actual_idx = start_idx + row_index
                    html_row = rows[actual_idx] if actual_idx < len(rows) else None
            
            if html_row:
                # Use recursive=True to catch all cells, then filter nested tables
                # Since we're using html_row.find_all(), all returned cells are descendants of html_row
                # We only need to filter out cells that belong to nested tables
                html_cells = html_row.find_all(['td', 'th'], recursive=True)
                html_cells = [c for c in html_cells if c.find_parent('table') == html_table]
                
                # Process each cell
                for cell_idx, cell_value in enumerate(row_data):
                    if cell_idx < len(html_cells):
                        html_cell = html_cells[cell_idx]
                        processed_cell = self._process_cell(cell_value, html_cell)
                        processed_cells.append(processed_cell)
                    else:
                        processed_cells.append(str(cell_value))
            else:
                # No matching HTML row, return as-is
                processed_cells = [str(cell) for cell in row_data]
        
        except Exception:
            # If processing fails, return original row data
            processed_cells = [str(cell) for cell in row_data]
        
        return processed_cells
    
    def _process_cell(self, cell_text, html_cell):
        """
        Process a single table cell, converting HTML links to markdown.
        
        Args:
            cell_text: Plain text content of the cell
            html_cell: BeautifulSoup cell element
            
        Returns:
            Processed cell text with links as markdown
        """
        if not html_cell:
            return str(cell_text)
        
        try:
            # Find all links in the cell
            links = html_cell.find_all('a', href=True)
            
            if not links:
                return str(cell_text)
            
            # Convert links to markdown
            markdown_links = []
            for link in links:
                href = link.get('href', '').strip()
                link_text = link.get_text(strip=True) or href
                
                if href.startswith('mailto:'):
                    email = unescape(href[7:])
                    markdown_links.append(f"[{link_text}](mailto:{email})")
                elif href:
                    markdown_links.append(f"[{link_text}]({href})")
            
            # If we have links, return them (prioritize email links)
            if markdown_links:
                email_links = [l for l in markdown_links if 'mailto:' in l]
                if email_links:
                    return email_links[0] if len(email_links) == 1 else " | ".join(email_links)
                else:
                    return " | ".join(markdown_links)
            else:
                return str(cell_text)
        
        except Exception:
            return str(cell_text)


def format_cell_with_links(cell_content, cell_html=None):
    """
    Format a table cell, preserving links and emails as markdown.
    
    This function ensures that hyperlinks and email addresses within table cells
    are preserved in the markdown output. If cell_html is provided, it extracts
    links and emails from the HTML. Otherwise, it processes the text content.
    
    Args:
        cell_content: Plain text content of the cell
        cell_html: Optional HTML content of the cell (for link extraction)
    
    Returns:
        Formatted markdown string with links preserved
    """
    if not cell_content:
        return ""
    
    # If HTML is available, extract links and emails from it
    if cell_html and BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(cell_html, 'lxml')
            cell_text = soup.get_text(strip=True)
            
            # Extract all links (both <a> tags and mailto: links)
            links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').strip()
                link_text = link.get_text(strip=True) or href
                
                # Handle email links (mailto:) - this is critical for preserving emails
                if href.startswith('mailto:'):
                    email = unescape(href[7:])
                    # For email links, use the email address as the link text if link_text is just the name
                    # Format: [Name](mailto:email@desy.de)
                    links.append(f"[{link_text}](mailto:{email})")
                # Handle regular links
                elif href:
                    # Make relative URLs absolute if needed
                    if href.startswith('/'):
                        # Keep as-is for now (could make absolute if base URL available)
                        links.append(f"[{link_text}]({href})")
                    elif href.startswith('http'):
                        links.append(f"[{link_text}]({href})")
                    else:
                        links.append(f"[{link_text}]({href})")
            
            # If we found links, prioritize links over plain text
            # For email cells, the link IS the content, so return just the link
            if links:
                # For email links, return just the markdown link (not combined with text)
                # This ensures emails appear as [Name](mailto:email@desy.de) instead of "Name | [Name](mailto:email@desy.de)"
                email_links = [l for l in links if 'mailto:' in l]
                if email_links:
                    # If we have email links, return them (usually just one)
                    return email_links[0] if len(email_links) == 1 else " | ".join(email_links)
                else:
                    # Regular links - combine with text if text is different
                    if cell_text and cell_text.strip():
                        # Check if cell_text matches any link text
                        link_texts = [l.split(']')[0].replace('[', '').strip() for l in links]
                        if cell_text.strip() not in link_texts:
                            # Text is different from link text, combine them
                            return f"{cell_text} | " + " | ".join(links)
                    return " | ".join(links)
            else:
                return cell_text or str(cell_content)
        except Exception:
            # Fallback to plain text if HTML parsing fails
            return str(cell_content)
    else:
        # No HTML available, check if text contains email pattern
        text = str(cell_content).strip()
        
        # Try to detect email in plain text
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        if emails:
            # Replace email with markdown link
            for email in emails:
                text = text.replace(email, f"[{email}](mailto:{email})")
        
        return text


def is_pdf_url(url):
    """Delegate to url_utils."""



# ============================================================================
# Cell extraction and table enrichment functions
# ============================================================================
def extract_cell_links(cell_element):
    """
    Extract ALL content from a table cell (text + links) and return as markdown.
    
    GENERAL-PURPOSE STRATEGY:
    1. Remove images (they're decorative, not content)
    2. Convert all links to markdown format, preserving their text content
    3. Extract all remaining text
    4. Combine everything in order
    
    This ensures names, emails, phone numbers, and all text are preserved.
    
    Args:
        cell_element: BeautifulSoup element representing a table cell
        
    Returns:
        Markdown string with all content preserved (text + links)
    """
    if not cell_element:
        return ""
    
    try:
        # Create a working copy to avoid modifying the original
        cell_html = str(cell_element)
        cell_copy = BeautifulSoup(cell_html, 'lxml')
        cell = cell_copy
        
        # Step 1: Remove all images (decorative, not content)
        # But preserve any text that might be associated with them
        for img in cell.find_all('img'):
                img.decompose()
        
        # Step 2: Process all links and convert to markdown
        # This preserves the link text (which often contains names)
        links = cell.find_all('a', href=True, recursive=True)
        from bs4 import NavigableString
        
        for link in links:
            href = link.get('href', '').strip()
            if not href:
                link.decompose()  # Remove empty links
                continue
            
            # Get link text - this is critical for preserving names
            link_text = link.get_text(strip=True)
            
            # If no link text, try to get it from attributes
            if not link_text or len(link_text) < 1:
                link_text = (link.get('title') or link.get('aria-label') or '').strip()
            
            # Handle mailto links
            if href.startswith('mailto:'):
                email = unescape(href[7:])
                # Use email as text if link text is generic or missing
                if not link_text or link_text.lower() in ['email', 'e-mail', 'mail', 'contact', 'e-mail:']:
                    link_text = email
                markdown_link = f"[{link_text}](mailto:{email})"
            elif href:
                # Regular link - use link text or href as fallback
                if not link_text:
                        link_text = href
                markdown_link = f"[{link_text}]({href})"
            else:
                link.decompose()
                continue
            
            # Replace link with markdown (preserves link text)
            link.replace_with(NavigableString(markdown_link))
        
        # Step 3: Extract all text (includes markdown links we just inserted)
        # Use space separator to keep words together but separate elements
        cell_text = cell.get_text(separator=' ', strip=True)
        
        # Step 4: Clean up whitespace


        # Use unicode-aware regex to handle umlauts and special characters correctly
        cell_text = re.sub(r'\s+', ' ', cell_text, flags=re.UNICODE).strip()
        
        # Step 5: Remove duplicate email links (if same email appears multiple times)
        email_pattern = r'\[([^\]]+)\]\(mailto:([^\)]+)\)'
        emails_seen = set()
        def dedup_emails(match):
            email = match.group(2).lower()
            if email in emails_seen:
                return match.group(2)  # Just return email, not full link
            emails_seen.add(email)
            return match.group(0)  # Keep full markdown link
        
        cell_text = re.sub(email_pattern, dedup_emails, cell_text)
        
        # Step 6: Add labels if content suggests them (phone, location, email)
        # Only add if label is missing
        original_html = str(cell_element)
        
        # Email label
        if re.search(r'\[([^\]]+)\]\(mailto:[^\)]+\)', cell_text):
            if not re.search(r'(?:E-Mail|E-mail|Email|e-mail)[:\s]', cell_text, re.IGNORECASE):
                cell_text = re.sub(r'(\[([^\]]+)\]\(mailto:[^\)]+\))', r'E-mail: \1', cell_text, count=1)
        
        # Phone label
        phone_pattern = r'(\+?\d{1,3}[\s\-\(\)]*(?:0\))?\s*\d{1,4}[\s\-]+\d{3,4}[\s\-]+\d{3,4})'
        phone_match = re.search(phone_pattern, cell_text)
        if phone_match:
            phone_text = phone_match.group(1)
            digit_count = len(re.findall(r'\d', phone_text))
            # Valid phone: at least 8 digits, not a year
            if digit_count >= 8 and not re.match(r'^(19|20)\d{2}', phone_text.replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+', '')):
                before_phone = cell_text[:phone_match.start()]
                if not re.search(r'(?:Phone|Tel|Telephone)[:\s]', before_phone, re.IGNORECASE):
                    cell_text = cell_text[:phone_match.start()] + 'Phone: ' + phone_text + cell_text[phone_match.end():]
        
        # Location label
        location_pattern = r'\b([A-Z]\d+[A-Z]?\s*[/-]\s*\d+[A-Z]?|[A-Z]\d+[a-z]?\s*/\s*[A-Z]{1,3}\.?\d+)'
        location_match = re.search(location_pattern, cell_text)
        if location_match:
            location_text = location_match.group(1)
            # Exclude publication IDs and dates
            if not re.search(r'PUBDB|PUB|ID|DOI|ISBN', location_text, re.IGNORECASE):
                if not re.match(r'^\d{4}[\s\-]+\d{4}', location_text):
                    before_location = cell_text[:location_match.start()]
                    if not re.search(r'(?:Location|Office|Room)[:\s]', before_location, re.IGNORECASE):
                        cell_text = cell_text[:location_match.start()] + 'Location: ' + location_text + cell_text[location_match.end():]
        
        # Step 7: Remove duplicate consecutive words (but not names before links)
        cell_text = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_text)
        
        return cell_text if cell_text else ""
        
    except Exception as e:
        # Fallback: just get text content
        try:
            return cell_element.get_text(strip=True)
        except:
            return ""


def enrich_crawl4ai_tables_with_links(result, is_pdf=False):
    """
    Method 1: Extract tables using Crawl4AI's built-in table extraction,
    then enrich them with links from the original HTML.
    
    This preserves Crawl4AI's table structure and formatting while adding
    back the links that were lost during markdown conversion.
    
    Args:
        result: Crawl4AI result object
        is_pdf: Whether this is a PDF result
        
    Returns:
        Markdown string with formatted tables with links preserved
    """
    tables_markdown = ""
    
    try:
        # Get Crawl4AI's extracted tables
        if not hasattr(result, 'tables') or not result.tables:
            return ""
        
        tables_markdown = "\n\n## Extracted Tables\n\n"
        
        # Get HTML tables for link enrichment (only for HTML pages)
        html_tables = []
        if not is_pdf and hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
            try:
                soup = BeautifulSoup(result.html, 'lxml')
                html_tables = soup.find_all('table', recursive=True)
            except Exception:
                pass
        
        # Process each Crawl4AI table
        used_html_tables = set()  # Track which HTML tables we've already used
        
        for idx, crawl_table in enumerate(result.tables, 1):
            tables_markdown += f"### Table {idx}\n\n"
            
            # Try to find corresponding HTML table to enrich with links
            # Match by content similarity rather than just index
            enriched_table = None
            if html_tables:
                # Get a sample of text from Crawl4AI table for matching
                crawl_sample = ""
                if crawl_table.get('rows'):
                    # Use first few cells from first row as identifier
                    first_row = crawl_table.get('rows', [])[0]
                    crawl_sample = " ".join(str(cell)[:30] for cell in first_row[:3] if cell)
                
                # Find best matching HTML table
                best_match_idx = None
                best_match_score = 0
                
                for html_idx, html_table in enumerate(html_tables):
                    if html_idx in used_html_tables:
                        continue
                    
                    # Get sample text from HTML table
                    html_sample = ""
                    tbody = html_table.find('tbody')
                    table_rows = tbody.find_all('tr') if tbody else html_table.find_all('tr')
                    if table_rows:
                        first_row = table_rows[0]
                        cells = first_row.find_all(['td', 'th'], limit=3)
                        html_sample = " ".join(cell.get_text(strip=True)[:30] for cell in cells)
                    
                    # Simple similarity: check if crawl_sample appears in html_sample or vice versa
                    if crawl_sample and html_sample:
                        # Count common words
                        crawl_words = set(crawl_sample.lower().split())
                        html_words = set(html_sample.lower().split())
                        common = len(crawl_words & html_words)
                        if common > best_match_score:
                            best_match_score = common
                            best_match_idx = html_idx
                
                # Use best match if found, otherwise try index-based matching
                if best_match_idx is not None and best_match_score > 0:
                    html_table = html_tables[best_match_idx]
                    used_html_tables.add(best_match_idx)
                    enriched_table = enrich_table_with_html_links(crawl_table, html_table)
                elif idx <= len(html_tables) and (idx - 1) not in used_html_tables:
                    # Fallback to index-based matching
                    html_table = html_tables[idx - 1]
                    used_html_tables.add(idx - 1)
                    enriched_table = enrich_table_with_html_links(crawl_table, html_table)
            
            # Use enriched table if available, otherwise use Crawl4AI's original
            table_to_use = enriched_table if enriched_table else crawl_table
            
            # Extract table data
            headers = table_to_use.get('headers', [])
            rows = table_to_use.get('rows', [])
            caption = table_to_use.get('caption', '')
            
            if caption:
                tables_markdown += f"*{caption}*\n\n"
            
            # Format as markdown table
            if headers and rows:
                tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                
                for row in rows:
                    row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
                    tables_markdown += "| " + " | ".join(str(cell) for cell in row_data) + " |\n"
                tables_markdown += "\n"
            elif rows:
                if rows:
                    first_row = rows[0]
                    tables_markdown += "| " + " | ".join(str(cell) for cell in first_row) + " |\n"
                    tables_markdown += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                    for row in rows[1:]:
                        tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                    tables_markdown += "\n"
        
        # Also check for tables that Crawl4AI might have missed (both nested and top-level)
        # Only extract tables that have meaningful content
        if html_tables:
            missed_count = 0
            for html_idx, html_table in enumerate(html_tables):
                if html_idx in used_html_tables:
                    continue
                
                # Extract table (whether nested or top-level)
                missed_table = extract_table_from_html(html_table)
                headers = missed_table.get('headers', [])
                rows = missed_table.get('rows', [])
                
                # Only include if it has meaningful content (at least 2 rows or headers)
                if (headers and rows) or (rows and len(rows) > 1):
                    missed_count += 1
                    # Check if this is a nested table
                    parent_table = html_table.find_parent('table')
                    table_label = "Nested" if parent_table else "Missed"
                    tables_markdown += f"### Table {len(result.tables) + missed_count} ({table_label})\n\n"
                    
                    caption = missed_table.get('caption', '')
                    if caption:
                        tables_markdown += f"*{caption}*\n\n"
                    
                    if headers and rows:
                        tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                        tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                        for row in rows:
                            row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
                            tables_markdown += "| " + " | ".join(str(cell) for cell in row_data) + " |\n"
                        tables_markdown += "\n"
                    elif rows and len(rows) > 1:
                        first_row = rows[0]
                        tables_markdown += "| " + " | ".join(str(cell) for cell in first_row) + " |\n"
                        tables_markdown += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                        for row in rows[1:]:
                            tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                        tables_markdown += "\n"
            
    except Exception as e:
        tables_markdown = f"\n\n## Extracted Tables\n\n*Error extracting tables: {e}*\n\n"
        import traceback
        traceback.print_exc()
    
    return tables_markdown


def enrich_table_with_html_links(crawl_table, html_table):
    """
    Enrich Crawl4AI's extracted table with links from HTML table.
    
    Args:
        crawl_table: Table dict from Crawl4AI
        html_table: BeautifulSoup table element
        
    Returns:
        Enriched table dict with links preserved
    """
    enriched = {
        'headers': [],
        'rows': [],
        'caption': crawl_table.get('caption', '')
    }
    
    # Enrich headers - use recursive=True to catch all header cells
    html_headers = []
    thead = html_table.find('thead')
    if thead:
        html_headers = thead.find_all(['th', 'td'], recursive=True)
        # Filter to ensure headers belong to this table, not nested tables
        html_headers = [h for h in html_headers if h.find_parent('table') == html_table]
    else:
        first_row = html_table.find('tr')
        if first_row:
            html_headers = first_row.find_all(['th', 'td'], recursive=True)
            # Filter to ensure headers belong to this table, not nested tables
            html_headers = [h for h in html_headers if h.find_parent('table') == html_table]
    
    crawl_headers = crawl_table.get('headers', [])
    for i, crawl_header in enumerate(crawl_headers):
        if i < len(html_headers):
            enriched['headers'].append(extract_cell_links(html_headers[i]))
        else:
            enriched['headers'].append(str(crawl_header))
    
    # Enrich rows - use recursive=True to catch all rows
    tbody = html_table.find('tbody')
    if tbody:
        table_rows = tbody.find_all('tr', recursive=True)
        # Filter to ensure rows belong to this table, not nested tables
        table_rows = [r for r in table_rows if r.find_parent('table') == html_table]
    else:
        table_rows = html_table.find_all('tr', recursive=True)
        # Filter to ensure rows belong to this table, not nested tables
        table_rows = [r for r in table_rows if r.find_parent('table') == html_table]
        # Exclude header rows
        if thead:
            thead_rows = thead.find_all('tr', recursive=True)
            thead_rows = [r for r in thead_rows if r.find_parent('table') == html_table]
            thead_row_set = set(thead_rows)
            table_rows = [r for r in table_rows if r not in thead_row_set]
        elif html_headers:
            # Headers were in first row, skip it
            table_rows = table_rows[1:] if len(table_rows) > 1 else []
    
    start_idx = 0  # Already filtered header rows above
    
    crawl_rows = crawl_table.get('rows', [])
    html_rows = table_rows[start_idx:]
    
    for row_idx, crawl_row in enumerate(crawl_rows):
        if row_idx < len(html_rows):
            html_row = html_rows[row_idx]
            # Use recursive=True to catch all cells, then filter nested tables
            # Since we're using html_row.find_all(), all returned cells are descendants of html_row
            # We only need to filter out cells that belong to nested tables
            html_cells = html_row.find_all(['td', 'th'], recursive=True)
            html_cells = [c for c in html_cells if c.find_parent('table') == html_table]
            enriched_row = []
            for cell_idx, crawl_cell in enumerate(crawl_row):
                if cell_idx < len(html_cells):
                    enriched_row.append(extract_cell_links(html_cells[cell_idx]))
                else:
                    enriched_row.append(str(crawl_cell))
            enriched['rows'].append(enriched_row)
        else:
            enriched['rows'].append([str(cell) for cell in crawl_row])
    
    return enriched


def extract_table_from_html(html_table):
    """
    Extract a complete table structure from HTML table element.
    
    Args:
        html_table: BeautifulSoup table element
        
    Returns:
        Table dict with headers, rows, and caption
    """
    table_data = {
        'headers': [],
        'rows': [],
        'caption': ''
    }
    
    # Extract caption
    caption = html_table.find('caption')
    if caption:
        table_data['caption'] = caption.get_text(strip=True)
    
    # Extract headers - be conservative: only use explicit headers
    # Headers should be in <thead> or use <th> tags, not inferred from first row
    # Use recursive=True first to catch all header cells, then filter nested tables
    thead = html_table.find('thead')
    if thead:
        # Explicit <thead> section - definitely headers
        # Use recursive=True to catch all header cells wrapped in other elements
        header_cells = thead.find_all(['th', 'td'], recursive=True)
        # Filter out cells from nested tables
        header_cells = [c for c in header_cells if c.find_parent('table') == html_table]
        # If no cells found recursively, try direct children as fallback
        if not header_cells:
            header_cells = thead.find_all(['th', 'td'], recursive=False)
        table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
    else:
        # No <thead> - check if first row uses <th> tags (strong indicator of headers)
        # Use recursive=True first to catch all rows
        first_row = html_table.find('tr', recursive=True)
        if first_row:
            # Ensure first_row belongs to this table, not a nested table
            if first_row.find_parent('table') != html_table:
                # Find first row that belongs to this table
                all_rows = html_table.find_all('tr', recursive=True)
                all_rows = [r for r in all_rows if r.find_parent('table') == html_table]
                first_row = all_rows[0] if all_rows else None
        
        if first_row:
            # Use recursive=True to catch all cells wrapped in other elements
            # Since we're using first_row.find_all(), all returned cells are descendants of first_row
            # We only need to filter out cells that belong to nested tables
            header_cells = first_row.find_all(['th', 'td'], recursive=True)
            # Filter out cells from nested tables
            header_cells = [c for c in header_cells if c.find_parent('table') == html_table]
            # If no cells found recursively, try direct children as fallback
            if not header_cells:
                header_cells = first_row.find_all(['th', 'td'], recursive=False)
            
            # Only treat as headers if ALL cells in first row are <th> tags
            # This avoids misidentifying data rows as headers
            if header_cells and all(cell.name == 'th' for cell in header_cells):
                
                table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
            # Additional check: if first row has <th> tags mixed with <td>, 
            # only use the <th> cells as headers (common in complex tables)
            elif header_cells:
                th_cells = [cell for cell in header_cells if cell.name == 'th']
                if th_cells and len(th_cells) >= len(header_cells) * 0.5:  # At least 50% are <th>
                    table_data['headers'] = [extract_cell_links(cell) for cell in header_cells]
    
    # Extract rows - GENERAL STRATEGY: Find all rows, exclude only clearly nested ones
    tbody = html_table.find('tbody')
    if tbody:
        # Has tbody - get rows from tbody
        table_rows = tbody.find_all('tr', recursive=True)
        # Filter: only exclude rows that are clearly in nested tables
        filtered_rows = []
        for r in table_rows:
            parent_table = r.find_parent('table')
            # Include if: belongs to html_table OR parent is None (might be in html_table structure)
            # Exclude only if: parent is a nested table (parent_table is inside html_table)
            if parent_table and parent_table != html_table:
                if parent_table.find_parent('table') == html_table:
                    continue  # Skip nested table rows
            filtered_rows.append(r)
        table_rows = filtered_rows
        # Fallback: if no rows found recursively, try direct children
        if not table_rows:
            table_rows = tbody.find_all('tr', recursive=False)
    else:
        # No tbody - get all tr elements
        all_rows = html_table.find_all('tr', recursive=True)
        # Filter: only exclude rows that are clearly in nested tables
        filtered_rows = []
        for r in all_rows:
            parent_table = r.find_parent('table')
            if parent_table and parent_table != html_table:
                if parent_table.find_parent('table') == html_table:
                    continue  # Skip nested table rows
            filtered_rows.append(r)
        all_rows = filtered_rows
        # Fallback: if no rows found recursively, try direct children
        if not all_rows:
            all_rows = html_table.find_all('tr', recursive=False)
        
        # Exclude header rows if we found headers
        if thead:
            thead_rows = thead.find_all('tr', recursive=True)
            if not thead_rows:
                thead_rows = thead.find_all('tr', recursive=False)
            thead_row_set = set(thead_rows)
            table_rows = [r for r in all_rows if r not in thead_row_set]
        elif table_data['headers']:
            # Headers were in first row, skip it
            table_rows = all_rows[1:] if len(all_rows) > 1 else []
        else:
            table_rows = all_rows
    
    # Extract data from each row - GENERAL STRATEGY: Get all cells, exclude only nested ones
    for tr in table_rows:
        # CRITICAL FIX: Try direct children first (most accurate for table structure)
        # Only use recursive=True if direct children don't exist
        cells = tr.find_all(['td', 'th'], recursive=False)
        
        
        
        # If no direct children, try recursive but filter nested tables
        if not cells:
            cells = tr.find_all(['td', 'th'], recursive=True)
            # Filter: only exclude cells that are clearly in nested tables
            filtered_cells = []
            for c in cells:
                parent_table = c.find_parent('table')
                # Include if: belongs to html_table OR parent is None
                # Exclude only if: parent is a nested table
                if parent_table and parent_table != html_table:
                    if parent_table.find_parent('table') == html_table:
                        continue  # Skip nested table cells
                filtered_cells.append(c)
            cells = filtered_cells
        else:
            # Direct children found - but still filter out any that might be in nested tables
            filtered_cells = []
            for c in cells:
                parent_table = c.find_parent('table')
                if parent_table and parent_table != html_table:
                    continue  # Skip if in nested table
                filtered_cells.append(c)
            cells = filtered_cells
        
        
        
        # Extract cell content
        if cells:
            row_data = []
            for cell in cells:
                cell_content = extract_cell_links(cell)
                # Use unicode-aware regex to handle umlauts and special characters correctly
                cell_content = re.sub(r'\s+', ' ', cell_content, flags=re.UNICODE).strip()
                row_data.append(cell_content)
            
            # Only skip rows where ALL cells are completely empty
            if any(str(cell).strip() for cell in row_data):
                table_data['rows'].append(row_data)
    
    return table_data



# ============================================================================
# Single-column table parsing and normalization
# ============================================================================
def parse_single_column_cell_html(cell_element):
    """
    Parse single-column cell HTML preserving structure (<br> tags, links).
    
    Extracts content from HTML cell element while preserving:
    - <br> tags as line breaks
    - Links as markdown format
    - HTML structure for delimiter detection
    
    Args:
        cell_element: BeautifulSoup cell element (td or th)
        
    Returns:
        Dict with:
        - 'text': Text content with <br> replaced by newlines
        - 'html_structure': List of segments with their types (text, link, br)
        - 'links': List of markdown links found
    """
    if not cell_element:
        return {'text': '', 'html_structure': [], 'links': []}
    
    try:
        # Create working copy
        cell_html = str(cell_element)
        cell_copy = BeautifulSoup(cell_html, 'lxml')
        cell = cell_copy
        
        # Remove images (decorative)
        for img in cell.find_all('img'):
            img.decompose()
        
        # Process links and convert to markdown (preserve for later)
        links = []
        link_markdown_map = {}
        for link in cell.find_all('a', href=True, recursive=True):
            href = link.get('href', '').strip()
            if not href:
                continue
            
            link_text = link.get_text(strip=True)
            if not link_text:
                link_text = (link.get('title') or link.get('aria-label') or '').strip()
            
            if href.startswith('mailto:'):
                email = unescape(href[7:])
                if not link_text or link_text.lower() in ['email', 'e-mail', 'mail', 'contact', 'e-mail:']:
                    link_text = email
                markdown_link = f"[{link_text}](mailto:{email})"
            elif href:
                if not link_text:
                    link_text = href
                markdown_link = f"[{link_text}]({href})"
            else:
                continue
    
            # Store mapping for replacement
            link_markdown_map[str(link)] = markdown_link
            links.append(markdown_link)
            
            # Replace link with placeholder to preserve position
            link.replace_with(NavigableString(f"__LINK_{len(links)-1}__"))
        
        # FIX 3: Flatten HTML structure before extracting text to avoid structural newlines
        # Replace nested divs/spans with spaces, but preserve <br> tags as newlines
        # This prevents structural newlines from breaking content parsing
        
        # First, replace <br> tags with a special marker (we'll convert to newline later)
        for br in cell.find_all('br'):
            br.replace_with(NavigableString('__BR__'))
        
        # Flatten nested block elements (div, p, span) by replacing with spaces
        # This prevents structural newlines from nested HTML
        # Process in reverse order to avoid modifying parent while iterating
        for block_elem in reversed(list(cell.find_all(['div', 'p', 'span']))):
            # Get text content
            block_text = block_elem.get_text(strip=True)
            if block_text:
                # Replace block element with its text content (space-separated)
                # Ensure proper spacing: add space before and after to prevent concatenation
                block_elem.replace_with(NavigableString(' ' + block_text + ' '))
            else:
                block_elem.decompose()
        
        # Now get text - use space separator to avoid structural newlines
        # FIX 3B: Remove ALL newlines first, then add back only intentional <br> newlines
        cell_text = cell.get_text(separator=' ', strip=False)
        
        # Remove all newlines (structural newlines from HTML)
        cell_text = re.sub(r'\n+', ' ', cell_text)
        
        # Replace BR markers with actual newlines (these are intentional line breaks)
        cell_text = cell_text.replace('__BR__', '\n')
        
        # Normalize multiple spaces to single space (but preserve intentional newlines from <br>)
        cell_text = re.sub(r'[ \t]+', ' ', cell_text)  # Multiple spaces/tabs -> single space
        cell_text = re.sub(r'\n\s+', '\n', cell_text)  # Remove leading spaces after newlines
        cell_text = re.sub(r'\s+\n', '\n', cell_text)  # Remove trailing spaces before newlines
        cell_text = re.sub(r' +', ' ', cell_text)  # Multiple spaces -> single space
        
        # Ensure proper spacing around common field labels (GENERALIZED: handles all variations)
        # This prevents concatenation like "AckermannE-Mail:" -> "Ackermann E-Mail:"
        # Also handle cases like "7415Location:" -> "7415 Location:"
        # Match all label variations: Tel, Telephone, Contact, Phone, Email, E-Mail, Location, Office, Room, etc.
        # FIX 3L: Don't match single digit + single letter (like "2A") - require 2+ chars for the label part
        cell_text = re.sub(r'([a-z0-9])([A-Z][a-z]{2,}:)', r'\1 \2', cell_text)  # lowercase/digit followed by 2+ letter Capital: -> add space
        # Handle specific multi-word labels
        cell_text = re.sub(r'([a-z0-9])(E-Mail:|E-mail:)', r'\1 \2', cell_text, flags=re.IGNORECASE)
        cell_text = re.sub(r'([a-z0-9])(Research Areas:)', r'\1 \2', cell_text, flags=re.IGNORECASE)
        # FIX 3I: Add space between number and letter (e.g., "02Fermi" -> "02 Fermi", "2L37" -> "2L 37")
        # But don't add space for single letter + number combinations like "2A" (location codes)
        # Only add space if it's a multi-digit number or multi-letter word
        cell_text = re.sub(r'(\d{2,})([A-Za-z])', r'\1 \2', cell_text)  # 2+ digits followed by letter
        cell_text = re.sub(r'([A-Za-z]{2,})(\d+)', r'\1 \2', cell_text)  # 2+ letters followed by number
        # Handle location codes like "2A / 02" - don't split "2A" but split "02Fermi"
        # This is more conservative and prevents breaking location codes
        
        # Restore markdown links
        for i, markdown_link in enumerate(links):
            cell_text = cell_text.replace(f"__LINK_{i}__", markdown_link)
        
        return {
            'text': cell_text,
            'links': links
        }
    except Exception as e:
        print(f"[DEBUG] parse_single_column_cell_html failed: {e}")
        return {'text': '', 'html_structure': [], 'links': []}


def normalize_field_label(label):
    """
    Normalize field labels to standardized headers for consistency across 200k+ URLs.
    
    Maps variations like Tel, Telephone, Contact, phone, Phone → Phone
    Maps variations like Email, e-mail, Mail → E-Mail
    Maps variations like Office, Room, Address → Location
    Maps variations like URL, Website, Homepage → Link
    Maps variations like Research Areas, Interests → Research Areas
    
    Args:
        label: Raw label string (may be empty, None, or contain variations)
        
    Returns:
        Normalized label string (standardized header name)
    """
    if not label or not isinstance(label, str):
        return label or ''
    
    label_lower = label.strip().rstrip(':').lower()
    
    # Phone variations: Tel, Telephone, Contact, T., etc. → Phone
    if label_lower in ['tel', 'telephone', 'telefon', 'contact', 't.', 'phone', 'mobile', 'cell', 'fax']:
        return 'Phone'
    
    # Email variations: Email, e-mail, Mail, etc. → E-Mail
    if label_lower in ['email', 'e-mail', 'e-mail:', 'mail', 'mail:', 'email address', 'e-mail address']:
        return 'E-Mail'
    
    # Location variations: Office, Room, Address, etc. → Location
    if label_lower in ['location', 'office', 'room', 'address', 'adresse', 'building', 'floor']:
        return 'Location'
    
    # Link variations: URL, Website, Homepage, etc. → Link
    if label_lower in ['link', 'url', 'website', 'homepage', 'web', 'home page', 'personal website']:
        return 'Link'
    
    # Research variations: Research Areas, Interests, etc. → Research Areas
    if label_lower in ['research', 'research areas', 'research area', 'interests', 'field', 'fields', 'focus', 'focus areas']:
        return 'Research Areas'
    
    # Name variations: Name, Full Name, etc. → Name
    if label_lower in ['name', 'full name', 'person', 'contact name']:
        return 'Name'
    
    # If no match, capitalize first letter of each word (Title Case)
    # This handles unknown labels gracefully
    words = label_lower.split()
    normalized = ' '.join(word.capitalize() for word in words)
    return normalized


def parse_single_column_table_content(cell_html):
    """
    Parse single-column cell content into Label | Value pairs using pattern-based heuristics.
    
    Strategy:
    1. Detect patterns (email, phone, URL) as primary delimiters
    2. Use HTML structure (<br>, newlines) as secondary
    3. Use multiple spaces as tertiary
    4. First segment (if no pattern, short) = Name
    5. Subsequent segments = Label | Value pairs
    
    Args:
        cell_html: BeautifulSoup cell element (td or th)
        
    Returns:
        List of [label, value] pairs, with first being Name if applicable
    """
    if not cell_html:
        return []
    
    # Parse HTML preserving structure
    parsed = parse_single_column_cell_html(cell_html)
    cell_text = parsed['text']
    
    if _is_empty_or_whitespace(cell_text):
        return []
    
    # Pattern definitions (universal, not hardcoded keywords)
    patterns = {
        'email': r'\[([^\]]+)\]\(mailto:([^\s@]+@[^\s@]+\.[^\s)]+)\)|([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
        'phone': r'\+?\d{1,3}[\s\-\(\)]*(?:0\))?\s*\d{1,4}[\s\-]+\d{3,4}[\s\-]+\d{3,4}',
        'url': r'\[([^\]]+)\]\((https?://[^\s\)]+)\)|(https?://[^\s\)]+)',
    }
    
    # Find all pattern positions
    pattern_positions = []
    for pattern_type, pattern_regex in patterns.items():
        for match in re.finditer(pattern_regex, cell_text):
            # Check if there's a label before the pattern (within 30 chars)
            start_pos = max(0, match.start() - 30)
            before_pattern = cell_text[start_pos:match.start()]
            
            # Look for label pattern (text ending with colon, optionally with "E-Mail", "Phone", etc.)
            label_match = re.search(r'([A-Za-z\s\-]+):\s*$', before_pattern)
            label = None
            if label_match:
                label = label_match.group(1).strip()
            
            pattern_positions.append({
                'type': pattern_type,
                'start': match.start(),
                'end': match.end(),
                'match': match.group(0),
                'label': label,
                'label_start': match.start() - len(before_pattern) + (label_match.start() if label_match else 0) if label_match else None
            })
    
    # Sort by position
    pattern_positions.sort(key=lambda x: x['start'])
    
    # Split cell text into segments based on patterns
    segments = []
    last_pos = 0
    
    for pos_info in pattern_positions:
        label_start = pos_info.get('label_start')
        pattern_start = pos_info['start']
        
        if label_start is not None and label_start < pattern_start:
            # Label exists before pattern
            before_label = cell_text[last_pos:label_start].strip()
            if before_label:
                segments.append({
                    'type': 'text',
                    'content': before_label,
                    'position': last_pos
                })
            
            # Label text
            label_text = pos_info.get('label', '')
            if label_text:
                segments.append({
                    'type': 'label',
                    'content': label_text,
                    'position': label_start
                })
            
            last_pos = pattern_start
        else:
            # No label - text before pattern
            before_text = cell_text[last_pos:pattern_start].strip()
            if before_text:
                segments.append({
                    'type': 'text',
                    'content': before_text,
                    'position': last_pos
                })
        
        # Pattern itself
        segments.append({
            'type': pos_info['type'],
            'content': pos_info['match'],
            'position': pattern_start
        })
        
        last_pos = pos_info['end']
    
    # Remaining text after last pattern
    remaining = cell_text[last_pos:].strip()
    if remaining:
        segments.append({
            'type': 'text',
            'content': remaining,
            'position': last_pos
        })
    
    # If no patterns found, try HTML structure delimiters
    if not pattern_positions:
        # Try splitting on newlines
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        if len(lines) > 1:
            segments = [{'type': 'text', 'content': line, 'position': i} for i, line in enumerate(lines)]
        else:
            # Try multiple spaces (2+)
            parts = re.split(r'\s{2,}', cell_text)
            if len(parts) > 1:
                segments = [{'type': 'text', 'content': part.strip(), 'position': i} for i, part in enumerate(parts) if part.strip()]
            else:
                # Single segment - return as-is
                segments = [{'type': 'text', 'content': cell_text.strip(), 'position': 0}]
    
    # Convert segments to Label | Value pairs
    label_value_pairs = []
    name_field = None
    
    # FIX 3K: Check for name in the FULL cell text before pattern-based splitting
    # This prevents names from being fragmented by pattern detection
    # Look for name pattern at the start of the cell text (before any patterns)
    # Pattern: Name followed by label (with or without colon) or email/phone pattern
    # Try with colon first, then without colon
    name_pattern_with_colon = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website):'
    name_pattern_without_colon = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website)\s+'
    # Also try name followed directly by email link pattern
    name_pattern_email = r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,4})\s+\[([^\]]+)\]\(mailto:'
    
    name_match = re.search(name_pattern_with_colon, cell_text, re.IGNORECASE)
    if not name_match:
        name_match = re.search(name_pattern_without_colon, cell_text, re.IGNORECASE)
    if not name_match:
        name_match = re.search(name_pattern_email, cell_text, re.IGNORECASE)
    if name_match:
        name_candidate = name_match.group(1).strip()
        # Verify it's a valid name (2-4 words, no patterns)
        if (len(name_candidate.split()) >= 2 and len(name_candidate.split()) <= 4 and
            not re.search(patterns['email'], name_candidate) and
            not re.search(patterns['phone'], name_candidate)):
            name_field = name_candidate
            # Remove the name from the first text segment if it exists
            if segments and segments[0]['type'] == 'text':
                first_content = segments[0]['content']
                # Remove the name from the start of the first segment
                first_content = re.sub(r'^' + re.escape(name_candidate) + r'\s+', '', first_content, flags=re.IGNORECASE)
                if first_content.strip():
                    segments[0]['content'] = first_content.strip()
                else:
                    segments.pop(0)  # Remove empty segment
    
    # Fallback: Check first segment for Name field (if not already found)
    if not name_field and segments:
        first_seg = segments[0]
        first_content = first_seg['content']
        
        # FIX 3J: Extract name from text that may contain labels (e.g., "Markus Ackermann E-Mail" -> "Markus Ackermann")
        # Remove common label patterns from the end of the first segment
        name_candidate = first_content
        # Remove label patterns at the end (E-Mail, Phone, Location, etc.)
        name_candidate = re.sub(r'\s+(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room|Link|URL|Website):?\s*$', '', name_candidate, flags=re.IGNORECASE)
        name_candidate = name_candidate.strip()
        
        # Name if: no pattern, short (< 50 chars), and looks like a name (2-4 capitalized words)
        is_name_like = bool(re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', name_candidate))
        if (first_seg['type'] == 'text' and 
            len(name_candidate) < 50 and
            len(name_candidate) > 2 and  # At least 3 characters
            not re.search(r':\s*$', name_candidate) and
            not re.search(patterns['email'], name_candidate) and
            not re.search(patterns['phone'], name_candidate) and
            (is_name_like or (len(name_candidate.split()) >= 2 and len(name_candidate.split()) <= 4))):  # Require 2+ words
            name_field = name_candidate
            # If we extracted name from first segment, remove the original segment and continue
            segments = segments[1:]  # Remove from segments
    
    # Process remaining segments as Label | Value pairs
    i = 0
    while i < len(segments):
        seg = segments[i]
        
        if seg['type'] in ['email', 'phone', 'url']:
            # Pattern found - extract label and value
            label = ""
            value = seg['content']
            
            # Check if previous segment is a label
            if i > 0 and segments[i-1]['type'] == 'label':
                label = normalize_field_label(segments[i-1]['content'])
                segments.pop(i-1)
                i -= 1
            elif i > 0 and segments[i-1]['type'] == 'text':
                prev_text = segments[i-1]['content']
                if prev_text.endswith(':') and len(prev_text) < 30:
                    label = normalize_field_label(prev_text.rstrip(':').strip())
                    segments.pop(i-1)
                    i -= 1
                else:
                    # Use pattern type as label (normalized)
                    if seg['type'] == 'email':
                        label = 'E-Mail'
                    elif seg['type'] == 'phone':
                        label = 'Phone'
                    elif seg['type'] == 'url':
                        label = 'Link'
                    else:
                        label = seg['type'].capitalize()
            else:
                # Use pattern type as label (normalized)
                if seg['type'] == 'email':
                    label = 'E-Mail'
                elif seg['type'] == 'phone':
                    label = 'Phone'
                elif seg['type'] == 'url':
                    label = 'Link'
                else:
                    label = seg['type'].capitalize()
            
            # Check if next segment is continuation (text after pattern)
            # FIX 3D: Don't append if next text contains a field label (Location:, Phone:, E-Mail:, etc.)
            if i+1 < len(segments) and segments[i+1]['type'] == 'text':
                next_text = segments[i+1]['content']
                # Don't append if it contains a field label pattern - GENERALIZED: matches all variations
                has_field_label = bool(re.search(r'\b(Location|Office|Room|Address|Phone|Tel|Telephone|Contact|E-Mail|Email|Mail|Link|URL|Website|Homepage|Research|Research Areas|Interests|Name|Fax|Mobile|Cell):', next_text, re.IGNORECASE))
                # If short, doesn't look like new field, and doesn't contain field label, append to value
                if len(next_text) < 30 and not re.search(r':\s*$', next_text) and not has_field_label:
                    value = f"{value} {next_text}"
                    segments.pop(i+1)
            
            if label and value:
                label_value_pairs.append([label, value])
        
        elif seg['type'] == 'text':
            # Text segment - try to extract Label | Value
            text = seg['content']
            
            # FIX 3E: Check if text contains multiple field labels (e.g., "Location:2A / 02Fermi, Group Leader")
            # Split on field labels if present - GENERALIZED: matches all variations (Tel, Telephone, Contact, etc.)
            field_label_pattern = r'\b(Location|Office|Room|Address|Phone|Tel|Telephone|Contact|E-Mail|Email|Mail|Link|URL|Website|Homepage|Research|Research Areas|Interests|Name|Fax|Mobile|Cell):'
            if re.search(field_label_pattern, text, re.IGNORECASE):
                # Split text on field labels - capture label and following text
                # Pattern: (text before)(Label:)(value after label)
                parts = re.split(f'({field_label_pattern})\s*', text, flags=re.IGNORECASE)
                current_label = None
                current_value = []
                skip_next = False  # Skip the label part itself
                
                for i, part in enumerate(parts):
                    part = part.strip()
                    if not part:
                        continue
                    
                    # Check if this part is a field label (with colon)
                    label_match = re.match(field_label_pattern + r'\s*$', part, re.IGNORECASE)
                    if label_match:
                        # Save previous label/value pair if exists
                        if current_label and current_value:
                            value_text = ' '.join(current_value).strip()
                            # FIX 3F: Remove label word from value if it appears at start
                            # Prevents "Location 2A / 02" -> should be "2A / 02"
                            value_text = re.sub(r'^' + re.escape(current_label) + r'\s+', '', value_text, flags=re.IGNORECASE)
                            if value_text.strip():
                                label_value_pairs.append([current_label, value_text.strip()])
                        # Start new label - normalize it
                        raw_label = label_match.group(1)  # Get the label name (without colon)
                        current_label = normalize_field_label(raw_label)
                        current_value = []
                        skip_next = False
                    else:
                        # This is a value (text after label)
                        # FIX 3G: Remove label word from value if it appears
                        # Prevents "Location 2A / 02" when text is "Location:Location 2A / 02"
                        part_cleaned = part
                        if current_label:
                            # Remove label word from start of value
                            part_cleaned = re.sub(r'^' + re.escape(current_label) + r'\s+', '', part, flags=re.IGNORECASE)
                        # Only add if we have a current label (skip text before first label)
                        if current_label:
                            current_value.append(part_cleaned)
                        elif not current_label and i == 0:
                            # Text before first label - might be name or continuation
                            if not name_field and len(part) < 50:
                                name_field = part
                            else:
                                current_value.append(part)
                
                # Save last label/value pair
                if current_label and current_value:
                    value_text = ' '.join(current_value).strip()
                    # FIX 3F: Remove label word from value if it appears at start
                    value_text = re.sub(r'^' + re.escape(current_label) + r'\s+', '', value_text, flags=re.IGNORECASE)
                    if value_text.strip():
                        label_value_pairs.append([current_label, value_text.strip()])
            # Check if it contains colon (Label: Value format)
            elif ':' in text:
                parts = text.split(':', 1)
                if len(parts) == 2:
                    raw_label = parts[0].strip()
                    value = parts[1].strip()
                    if raw_label and value:
                        # Normalize label for consistency
                        label = normalize_field_label(raw_label)
                        label_value_pairs.append([label, value])
                else:
                    if text.strip():
                        label_value_pairs.append(['', text.strip()])
            else:
                # No colon - might be continuation of previous value
                if label_value_pairs:
                    label_value_pairs[-1][1] = f"{label_value_pairs[-1][1]} {text}".strip()
                else:
                    if not name_field:
                        name_field = text
                    else:
                        label_value_pairs.append(['', text])
        
        i += 1
    
    # Build result: Name first (if found), then Label | Value pairs
    result = []
    if name_field:
        result.append(['Name', name_field])
    result.extend(label_value_pairs)
    
    return result


def convert_single_column_to_multi_column_table(table_data, html_table_element):
    """
    Convert single-column table to multi-column format using pattern-based parsing.
    
    Args:
        table_data: Table dict with single-column rows (from extract_table_from_html)
        html_table_element: BeautifulSoup table element (for parsing HTML directly)
        
    Returns:
        Table dict with multi-column rows (Label | Value format)
    """
    if not table_data.get('rows'):
        return table_data
    
    # Check if this is a single-column table
    if not all(len(row) == 1 for row in table_data.get('rows', [])):
        return table_data  # Already multi-column
    
    # Parse each row from HTML directly (preserve structure)
    parsed_rows_data = []  # List of dicts: {label: value}
    all_labels_set = set()  # Collect all unique labels
    
    # Get all rows from HTML table
    html_rows = html_table_element.find_all('tr', recursive=False)
    if not html_rows:
        html_rows = html_table_element.find_all('tr', recursive=True)
        # Filter nested table rows
        html_rows = [r for r in html_rows if r.find_parent('table') == html_table_element]
    
    for html_row in html_rows:
        # Get single cell (single-column table)
        cells = html_row.find_all(['td', 'th'], recursive=False)
        if not cells:
            cells = html_row.find_all(['td', 'th'], recursive=True)
            cells = [c for c in cells if c.find_parent('table') == html_table_element]
        
        if len(cells) == 1:
            # Single-column cell - parse it
            cell_html = cells[0]
            label_value_pairs = parse_single_column_table_content(cell_html)
            
            if label_value_pairs:
                # Convert pairs to dict for easier handling
                row_dict = {}
                for label, value in label_value_pairs:
                    if label:  # Only add if label exists
                        # Normalize label for consistency across 200k+ URLs
                        normalized_label = normalize_field_label(label)
                        row_dict[normalized_label] = value
                        all_labels_set.add(normalized_label)
                    else:
                        # No label - might be continuation or standalone value
                        if row_dict:
                            # Append to last value
                            last_key = list(row_dict.keys())[-1]
                            row_dict[last_key] = f"{row_dict[last_key]} {value}".strip()
                        else:
                            # First item with no label - use "Info"
                            row_dict['Info'] = value
                            all_labels_set.add('Info')
                
                parsed_rows_data.append(row_dict)
            else:
                # Parsing failed - try to extract name from cell content before falling back to "Original"
                # This prevents "| Original |" rows when we can extract at least a name
                cell_text = extract_cell_links(cells[0])
                # Try to extract name pattern (capitalized words at start, including umlauts)
                name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){0,3})', cell_text, re.UNICODE)
                if name_match:
                    name = name_match.group(1).strip()
                    # Extract remaining content
                    remaining = cell_text[len(name):].strip()
                    if remaining:
                        parsed_rows_data.append({'Name': name, 'Info': remaining})
                        all_labels_set.add('Name')
                        all_labels_set.add('Info')
                    else:
                        parsed_rows_data.append({'Name': name})
                        all_labels_set.add('Name')
                else:
                    # No name found, use "Original" as fallback
                    parsed_rows_data.append({'Original': cell_text})
                    all_labels_set.add('Original')
        else:
            # Multi-column row (shouldn't happen in single-column table, but handle it)
            row_data = [extract_cell_links(cell) for cell in cells]
            # Convert to dict format for consistency
            row_dict = {}
            for i, cell_val in enumerate(row_data):
                label = f"Column {i+1}"
                row_dict[label] = cell_val
                all_labels_set.add(label)
            parsed_rows_data.append(row_dict)
    
    # If we successfully parsed at least one row, update table structure
    if parsed_rows_data and all_labels_set:
        # Create ordered header list (Name first if present, then others alphabetically)
        headers = []
        if 'Name' in all_labels_set:
            headers.append('Name')
            all_labels_set.remove('Name')
        # Add remaining labels in sorted order
        headers.extend(sorted(all_labels_set))
        
        # Convert dict rows to list rows matching header order
        parsed_rows = []
        for row_dict in parsed_rows_data:
            row_values = [row_dict.get(label, '') for label in headers]
            parsed_rows.append(row_values)
        
        table_data['rows'] = parsed_rows
        table_data['headers'] = headers
    
    return table_data



# ============================================================================
# DOM-order extraction, formatting, and link injection
# ============================================================================
def extract_headings_and_tables_in_dom_order(html_content, url=None):
    """
    Extract headings and tables in DOM order from rendered HTML.
    
    This function:
    1. Finds all headings (h1-h6) and tables in the HTML
    2. Sorts them by their position in the DOM
    3. Associates each table with its nearest preceding heading
    4. Returns a list of content items in DOM order
    
    Args:
        html_content: Rendered HTML string from Crawl4AI (result.html)
        url: Optional URL to enable PUBDB-specific filtering
        
    Returns:
        List of content items: [
            {'type': 'heading', 'level': 4, 'text': 'SCIENTISTS', 'position': 0},
            {'type': 'table', 'data': {...}, 'position': 1},
            ...
        ]
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return []
    
    try:
        
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Find main content area
        main_content_area = (soup.find('main') or 
                           soup.find('article') or 
                           soup.find('body') or
                           soup)
        
        
        
        # FIX 2B: Filter out navigation/header/footer headings if main_content_area is body
        # This prevents extracting navigation headings when body is used as fallback
        navigation_containers = ['nav', 'header', 'footer', 'aside']
        nav_elements = set()
        if main_content_area.name == 'body':
            # Filter by semantic HTML elements
            for nav_tag in navigation_containers:
                for nav_elem in soup.find_all(nav_tag, recursive=True):
                    for heading in nav_elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
            
            # Also filter by common navigation class/id patterns
            nav_patterns = [r'nav', r'sidebar', r'menu', r'header', r'footer', r'topbar', r'breadcrumb']
            for pattern in nav_patterns:
                for elem in soup.find_all(['div', 'section'], class_=re.compile(pattern, re.I), recursive=True):
                    for heading in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
                for elem in soup.find_all(['div', 'section'], id=re.compile(pattern, re.I), recursive=True):
                    for heading in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True):
                        nav_elements.add(heading)
        
        # Collect all headings, tables, AND paragraphs with their DOM positions
        # FIX: Also include 'p' elements to capture paragraph content (e.g., belle2.desy.de)
        all_elements = []
        
        # Elements to extract: headings, tables, and paragraphs with substantial content
        content_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table', 'p',  'ul', 'ol']  # 'ul', 'ol'
        
        
        # Find all content elements
        for elem in main_content_area.find_all(content_tags, recursive=True):
            # Skip navigation headings
            if elem.name.startswith('h') and elem in nav_elements:
                continue
            # Only process top-level tables (not nested)
            if elem.name == 'table' and elem.find_parent('table') is not None:
                continue
            # Process all paragraphs
            if elem.name == 'p':
                # Don't skip navigation paragraphs - let URL dedup handle redirects
                # Skip very short paragraphs (likely navigation/labels)
                text = elem.get_text(strip=True)
                # FIX: Allow short paragraphs that are questions or follow headings (common on landing pages)
                # Questions (ending with ?) are often descriptive text after headings
                is_question = text.endswith('?')
                # Check if previous sibling is a heading
                prev_sibling = elem.find_previous_sibling(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                is_after_heading = prev_sibling is not None
                
                # Lower threshold for questions or paragraphs after headings
                min_length = 15 if (is_question or is_after_heading) else 30
                if len(text) < min_length:
                    continue
                # Skip paragraphs that are just links
                links = elem.find_all('a')
                if links and len(text) < 50 and len(links) >= len(text.split()):
                    continue
            
            # FIX 2: Calculate position: count only previous elements within main_content_area
            # This prevents counting navigation/header elements that appear before main content
            position = 0
            # Get all elements in main_content_area in document order
            all_content_elems = main_content_area.find_all(content_tags, recursive=True)
            for prev_elem in all_content_elems:
                # Stop when we reach current element
                if prev_elem == elem:
                    break
                # Only count top-level tables (not nested)
                if prev_elem.name == 'table':
                    if prev_elem.find_parent('table') is None:
                        position += 1
                elif prev_elem.name.startswith('h'):
                    position += 1
                elif prev_elem.name == 'p':
                    # Only count substantial paragraphs
                    text = prev_elem.get_text(strip=True)
                    if len(text) >= 30 and not _is_in_navigation(prev_elem):
                        position += 1
                elif prev_elem.name in ['ul', 'ol']:
                    # Count lists (but skip if in navigation areas)
                    if not _is_in_navigation(prev_elem):
                        position += 1
            
            
            
            # Determine element type
            if elem.name.startswith('h'):
                elem_type = 'heading'
            elif elem.name == 'table':
                elem_type = 'table'
            elif elem.name == 'p':
                elem_type = 'paragraph'
            elif elem.name in ['ul', 'ol']: 
                elem_type = 'list'
            else:
                elem_type = 'text'
            
            all_elements.append({
                'element': elem,
                'type': elem_type,
                'position': position
            })
        
        # Sort by position
        all_elements.sort(key=lambda x: x['position'])
        
        
        
        # Process elements and extract content
        dom_ordered_content = []
        seen_headings = set()
        # FIX: Track table row text to avoid duplicating as paragraphs
        table_row_texts = set()
        
        for item in all_elements:
            elem = item['element']
            
            if item['type'] == 'heading':
                heading_text = elem.get_text(strip=True)
                
                if heading_text and heading_text not in seen_headings:
                    seen_headings.add(heading_text)
                    level = int(elem.name[1])
                    dom_ordered_content.append({
                        'type': 'heading',
                        'level': level,
                        'text': heading_text,
                        'position': item['position']
                    })
            
            elif item['type'] == 'table':
                # PUBDB-specific filtering: Only filter UI tables on PUBDB pages
                # Check both URL and content to handle redirects/embedded content
                if _is_pubdb_page(url, html_content):
                    table_text = elem.get_text(strip=True).lower()
                    
                    if is_pubdb_ui_table(table_text):
                        
                        continue  # Skip this UI table
                
                
                
                # Extract table data using existing function
                table_data = extract_table_from_html(elem)
                
                
                
                if table_data.get('rows'):
                    # Solution 3: Check if single-column table and convert to multi-column
                    is_single_column = all(len(row) == 1 for row in table_data.get('rows', []))
                    if is_single_column:
                        table_data = convert_single_column_to_multi_column_table(table_data, elem)
                    
                    
                    
                    dom_ordered_content.append({
                        'type': 'table',
                        'data': table_data,
                        'position': item['position']
                    })
                    
                    # FIX: Collect combined row text to avoid duplicating as paragraphs
                    # Individual cells may be short, but paragraphs often contain all cells concatenated
                    for row in table_data.get('rows', []):
                        # Combine all cells in this row
                        row_text = ' '.join(str(cell) for cell in row if cell)
                        if row_text:
                            # Normalize: lowercase, collapse whitespace, strip markdown links
                            row_text = row_text.lower()
                            row_text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', row_text)  # Remove markdown links
                            row_text = ' '.join(row_text.split())  # Normalize whitespace
                            if len(row_text) > 30:  # Only track substantial text
                                table_row_texts.add(row_text)
            
            elif item['type'] == 'paragraph':
                # Extract paragraph text with links preserved
                para_text = elem.get_text(separator=' ', strip=True)
                
                
                
                # FIX: Skip paragraphs whose text is already in a table row
                # Match based on key fields (names, emails, phone numbers) rather than exact text
                if para_text and table_row_texts:
                    para_normalized = para_text.lower()
                    para_normalized = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', para_normalized)  # Remove markdown links
                    para_normalized = re.sub(r'(e-?mail|phone|tel|location|office|room):\s*', '', para_normalized)  # Remove labels
                    para_normalized = ' '.join(para_normalized.split())  # Normalize whitespace
                    
                    # Check if paragraph shares significant content with any table row
                    is_duplicate = False
                    for row_text in table_row_texts:
                        # Extract key tokens (words 3+ chars, not common words)
                        para_tokens = set(w for w in para_normalized.split() if len(w) >= 3 and w not in ('the', 'and', 'for'))
                        row_tokens = set(w for w in row_text.split() if len(w) >= 3 and w not in ('the', 'and', 'for'))
                        
                        # If many tokens match, it's likely a duplicate
                        common_tokens = para_tokens & row_tokens
                        if len(common_tokens) >= 3 and len(common_tokens) >= len(para_tokens) * 0.5:
                            is_duplicate = True
                            
                            break
                    
                    if is_duplicate:
                        continue  # Skip this paragraph - it's duplicate table content
                
                # Also extract any links in the paragraph
                links = []
                for link in elem.find_all('a', href=True):
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)
                    if href and link_text:
                        # Convert to markdown link format
                        links.append((link_text, href))
                
                if para_text and len(para_text) >= 15: # 30:  # Only include substantial paragraphs
                    # Replace link text with markdown format in paragraph
                    formatted_text = para_text
                    for link_text, href in links:
                        if link_text in formatted_text:
                            formatted_text = formatted_text.replace(link_text, f"[{link_text}]({href})", 1)
                    
                    
                    
                    dom_ordered_content.append({
                        'type': 'paragraph',
                        'text': formatted_text,
                        'position': item['position']
                    })
            
            elif item['type'] == 'list':
                # Extract list items with links preserved
                list_items = []
                is_ordered = elem.name == 'ol'
                
                # Process all lists including navigation - let URL dedup handle redirects
                
                for li in elem.find_all('li', recursive=False):  # Only direct children
                    # Extract text and links from list item
                    item_text = li.get_text(separator=' ', strip=True)
                    if not item_text:
                        continue
                    
                    # Extract links and replace with markdown format
                    links = []
                    for link in li.find_all('a', href=True):
                        href = link.get('href', '')
                        link_text = link.get_text(strip=True)
                        if href and link_text:
                            links.append((link_text, href))
                    
                    # Replace link text with markdown format
                    formatted_item = item_text
                    for link_text, href in links:
                        if link_text in formatted_item:
                            formatted_item = formatted_item.replace(link_text, f"[{link_text}]({href})", 1)
                    
                    list_items.append(formatted_item)
                
                # Only add list if it has items
                if list_items:
                    dom_ordered_content.append({
                        'type': 'list',
                        'items': list_items,
                        'ordered': is_ordered,
                        'position': item['position']
                    })
        
        return dom_ordered_content
    except Exception as e:
        print(f"[DEBUG] extract_headings_and_tables_in_dom_order failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def format_tables_with_headings_as_markdown(dom_ordered_content):
    """
    Format headings and tables as markdown, preserving DOM order.
    
    Args:
        dom_ordered_content: List from extract_headings_and_tables_in_dom_order()
        
    Returns:
        Markdown string with headings and tables in DOM order
    """
    if not dom_ordered_content:
        return ""
    
    # Defensive check: ensure dom_ordered_content is a list
    if dom_ordered_content is None:
        return ""
    
    # GENERAL: Merge consecutive single-row tables with similar structure (same headers)
    # This handles cases where each row is in its own <table> element
    merged_content = []
    i = 0
    while i < len(dom_ordered_content):
        item = dom_ordered_content[i]
        
        if item['type'] == 'heading':
            merged_content.append(item)
            i += 1
        elif item['type'] == 'table':
            # Check if this is a single-row table that might be part of a larger table
            table_data = item.get('data', {}) or {}
            rows = table_data.get('rows', []) or []
            headers = table_data.get('headers', []) or []
            
            # Check if this is a single-row table with structured data (Name, E-mail, Phone, Location)
            is_single_row_structured = (len(rows) == 1 and 
                                       headers and 
                                       len(headers) >= 2 and
                                       any(label.lower() in ['name', 'e-mail', 'email', 'phone', 'location'] for label in headers))
            
            
            
            if is_single_row_structured:
                # Look ahead for consecutive single-row tables with same headers
                merge_candidates = [item]
                j = i + 1
                while j < len(dom_ordered_content):
                    next_item = dom_ordered_content[j]
                    
                    if next_item['type'] != 'table':
                        
                        break
                    
                    next_table_data = next_item.get('data', {}) or {}
                    next_rows = next_table_data.get('rows', []) or []
                    next_headers = next_table_data.get('headers', []) or []
                    
                    # Check if next table has same structure (same header types, single row)
                    # GENERAL: Headers may have person-specific values (e.g., "Andrey Siemens E-mail" vs "Anjali Panchwanee E-mail")
                    # So we check if headers have the same field types (E-mail, Location, Phone) rather than exact match
                    # Also allow different numbers of columns as long as field types match (some tables may be missing columns)
                    
                    if len(next_rows) == 1 and next_headers:
                        # Extract field types from headers (normalize to generic labels)
                        def extract_field_type(header):
                            header_lower = str(header).lower()
                            if 'e-mail' in header_lower or 'email' in header_lower:
                                return 'e-mail'
                            elif 'phone' in header_lower or 'tel' in header_lower:
                                return 'phone'
                            elif 'location' in header_lower or 'office' in header_lower or 'room' in header_lower:
                                return 'location'
                            elif 'name' in header_lower:
                                return 'name'
                            else:
                                return header_lower
                        
                        current_field_types = set(extract_field_type(h) for h in (headers or []))
                        next_field_types = set(extract_field_type(h) for h in (next_headers or []))
                        
                        # Merge if field types overlap (same structure, even if some columns are missing)
                        # Allow merging if at least 2 field types match (e.g., both have E-Mail and Location)
                        intersection = current_field_types & next_field_types
                        will_merge = len(intersection) >= 2
                        
                        
                        
                        if will_merge:
                            merge_candidates.append(next_item)
                            
                            j += 1
                        else:
                            
                            break
                    else:
                        # Next item is not a table or doesn't have single row, stop merging
                        break
                
                # If we found multiple tables to merge, merge them
                
                if len(merge_candidates) > 1:
                    # Merge all rows from candidates into one table
                    merged_rows = []
                    # Use normalized generic headers (extract field types, not person-specific values)
                    def normalize_header(header):
                        header_str = str(header).lower()
                        if 'e-mail' in header_str or 'email' in header_str:
                            return 'E-Mail'
                        elif 'phone' in header_str or 'tel' in header_str:
                            return 'Phone'
                        elif 'location' in header_str or 'office' in header_str or 'room' in header_str:
                            return 'Location'
                        elif 'name' in header_str:
                            return 'Name'
                        else:
                            # Keep original if no match
                            return str(header)
                    
                    # Collect all unique headers from all candidates to ensure we don't lose any columns
                    all_headers_set = set()
                    for candidate in merge_candidates:
                        candidate_data = candidate.get('data', {}) or {}
                        candidate_headers = candidate_data.get('headers', []) or []
                        for h in candidate_headers:
                            if h:  # Skip None headers
                                normalized_h = normalize_header(h)
                                all_headers_set.add(normalized_h)
                    
                    # FIX: If Name is not in headers but rows contain email links with names, infer Name column
                    # This handles cases where HTML structure doesn't have explicit Name header but email links contain names
                    if 'Name' not in all_headers_set:
                        # Check if any row has an email link with a name-like pattern
                        for candidate in merge_candidates:
                            candidate_data = candidate.get('data', {}) or {}
                            candidate_rows = candidate_data.get('rows', []) or []
                            for row in candidate_rows:
                                # Check all cells in the row for email links
                                for cell in row:
                                    cell_str = str(cell)
                                    # Look for markdown email link: [Name](mailto:...)
                                    email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', cell_str)
                                    if email_match:
                                        link_text = email_match.group(1).strip()
                                        # Validate it looks like a name (1-5 words, capitalized)
                                        words = link_text.split()
                                        if 1 <= len(words) <= 5:
                                            # Check if words look like names (start with capital or umlaut)
                                            is_name = all(
                                                w and (w[0].isupper() or w[0] in 'äöüÄÖÜ') 
                                                for w in words if w and not w.startswith('(')
                                            )
                                            # Check it's not a phone/number pattern
                                            has_phone = bool(re.search(r'\d{3,}|T\.|Phone', link_text))
                                            
                                            if is_name and not has_phone:
                                                # Found a name in email link - add Name to headers
                                                all_headers_set.add('Name')
                                                break
                                if 'Name' in all_headers_set:
                                    break
                            if 'Name' in all_headers_set:
                                break
                    
                    # Create ordered header list (Name first if present, then E-Mail, Phone, Location, then others)
                    normalized_headers = []
                    header_order = ['Name', 'E-Mail', 'Phone', 'Location']
                    for h in header_order:
                        if h in all_headers_set:
                            normalized_headers.append(h)
                            all_headers_set.remove(h)
                    # Add any remaining headers
                    normalized_headers.extend(sorted(all_headers_set))
                    
                    # Now merge rows, ensuring all columns are present
                    for candidate in merge_candidates:
                        candidate_data = candidate.get('data', {}) or {}
                        candidate_rows = candidate_data.get('rows', []) or []
                        candidate_headers = candidate_data.get('headers', []) or []
                        # Map candidate headers to normalized headers
                        candidate_header_map = {normalize_header(h): h for h in candidate_headers if h}
                        
                        if not candidate_rows:
                            continue  # Skip if no rows
                        
                        for row in candidate_rows:
                            # Create a new row with all normalized headers
                            new_row = []
                            for norm_h in normalized_headers:
                                # Find the original header that maps to this normalized header
                                orig_h = candidate_header_map.get(norm_h)
                                if orig_h and orig_h in candidate_headers:
                                    col_idx = candidate_headers.index(orig_h)
                                    if col_idx < len(row):
                                        new_row.append(row[col_idx])
                                    else:
                                        new_row.append('')
                                else:
                                    # Header not in original - might be inferred (like Name)
                                    # If this is Name and we have email links in the row, extract name from email
                                    if norm_h == 'Name':
                                        # Look for email link in any cell of this row
                                        name_extracted = False
                                        for cell in row:
                                            cell_str = str(cell)
                                            email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', cell_str)
                                            if email_match:
                                                link_text = email_match.group(1).strip()
                                                # Validate it looks like a name
                                                words = link_text.split()
                                                if 1 <= len(words) <= 5:
                                                    is_name = all(
                                                        w and (w[0].isupper() or w[0] in 'äöüÄÖÜ') 
                                                        for w in words if w and not w.startswith('(')
                                                    )
                                                    has_phone = bool(re.search(r'\d{3,}|T\.|Phone', link_text))
                                                    
                                                    if is_name and not has_phone:
                                                        new_row.append(link_text)
                                                        name_extracted = True
                                                        break
                                        if not name_extracted:
                                            new_row.append('')
                                    else:
                                        new_row.append('')
                            merged_rows.append(new_row)
                    
                    # Create merged table with normalized headers
                    merged_table = {
                        'type': 'table',
                        'data': {
                            'headers': normalized_headers,
                            'rows': merged_rows
                        },
                        'position': item['position']
                    }
                    merged_content.append(merged_table)
                    
                    i = j  # Skip all merged tables
                else:
                    # No merge needed, add as-is
                    merged_content.append(item)
                    i += 1
            else:
                # Not a candidate for merging, add as-is
                merged_content.append(item)
                i += 1
        elif item['type'] == 'paragraph':
            # Paragraphs don't need merging, just add them
            merged_content.append(item)
            i += 1
        else:
            merged_content.append(item)
            i += 1
    
    # Now format the merged content
    markdown_output = ""
    seen_table_signatures = set()  # For deduplication
    
    
    
    for item in merged_content:
        if item['type'] == 'heading':
            level = item['level']
            text = item['text']
            markdown_output += '#' * level + ' ' + text + "\n\n"
        
        elif item['type'] == 'table':
            table_data = item.get('data', {}) or {}
            headers = table_data.get('headers', []) or []
            rows = table_data.get('rows', []) or []
            
            
            
            if not rows:
                
                continue
            
            # GENERAL: Filter out trivial/small tables (single-row tables with just links, empty tables, etc.)
            # Skip tables that are too small or contain only links/empty cells
            # BUT: Allow single-row tables with 3+ columns that contain structured data (like staff/group member tables)
            if len(rows) <= 1:
                # Single-row table - check if it's just a link or trivial content
                first_row = rows[0] if rows else []
                # Count non-empty cells
                non_empty_cells = [str(c).strip() for c in first_row if str(c).strip()]
                num_cols = len(first_row)
                
                # GENERAL: Only filter if table has 1-2 non-empty cells AND they're just links
                # Tables with 3+ columns (even if single-row) may contain structured data (staff info, etc.)
                if len(non_empty_cells) <= 2 and num_cols <= 2:
                    # Check if cells contain mostly links/URLs
                    link_count = sum(1 for cell in non_empty_cells if re.search(r'\[.*?\]\(.*?\)|https?://', str(cell)))
                    if link_count >= len(non_empty_cells):
                        # This is a trivial link table - skip it
                        
                        continue
                # GENERAL: Filter out single-cell tables that are likely broken fragments
                # Pattern: Single-cell table with just a value (no label, no structure)
                if len(non_empty_cells) == 1:
                    single_cell = non_empty_cells[0]
                    # Skip if it's just a number/unit with no label (broken fragment)
                    if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)?\s*$', single_cell, re.I):
                        
                        continue
                # GENERAL: Filter out single-cell tables that are likely broken fragments
                # Pattern: Single-cell table with just a value (no label, no structure)
                if len(non_empty_cells) == 1:
                    single_cell = non_empty_cells[0]
                    # Skip if it's just a number/unit with no label (broken fragment)
                    if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)?\s*$', single_cell, re.I):
                        
                        continue
            
            # GENERAL: Filter out malformed tables (too many columns, concatenated data)
            # Pattern: Tables with 10+ columns are likely malformed (concatenated data)
            if rows and len(rows[0]) > 10:
                
                continue
            
            # Deduplication: Create signature from first few non-empty cells of first row
            # GENERAL: More robust signature to catch duplicates even with formatting differences
            sig_row = rows[0] if rows else []
            # Get first 3 non-empty cells (more robust than 2)
            sig_cells = [str(c).strip().lower()[:50] for c in sig_row[:3] if str(c).strip()]
            # Also include header signature if available
            if headers:
                header_sig = "|".join([str(h).strip().lower()[:30] for h in headers[:3] if str(h).strip()])
                table_sig = f"{header_sig}|{''.join(sig_cells)}"
            else:
                table_sig = "|".join(sig_cells)
            
            # GENERAL: Also check for malformed table signatures (concatenated data)
            # Only filter if table has many columns (10+) OR if it's single-column with field labels
            # Multi-column tables (2-10 columns) with field labels in cells are legitimate
            if table_sig:
                num_columns = len(rows[0]) if rows else 0
                field_labels_in_sig = len(re.findall(r'\b(e-mail|phone|location|email|tel|telephone):', table_sig, re.I))
                # Only filter if: (many columns AND field labels) OR (single-column with 3+ field labels)
                # This allows legitimate 2-column tables where cells contain structured data
                
                if (num_columns > 10 and field_labels_in_sig >= 3) or (num_columns == 1 and field_labels_in_sig >= 3):
                    
                    continue
            
            if table_sig and table_sig in seen_table_signatures:
                
                continue  # Skip duplicate
            
            if table_sig:
                seen_table_signatures.add(table_sig)
            
            
            
            # Format as markdown table
            if headers:
                # GENERAL: Check if detected headers are actually label-value pairs (common in parameter tables)
                # If headers look like labels (end with ":" or contain label words), treat them as data instead
                is_label_value_headers = False
                if len(headers) >= 2:
                    first_header = str(headers[0]).strip()
                    second_header = str(headers[1]).strip() if len(headers) > 1 else ""
                    # Check if first header ends with ":" (label indicator)
                    if first_header.endswith(':') or first_header.endswith('：'):
                        is_label_value_headers = True
                    # Also check if headers contain common label words
                    label_words = ['energy', 'circumference', 'number', 'length', 'angle', 'radius', 'field', 
                                   'aperture', 'gradient', 'emittance', 'tune', 'frequency', 'time', 'current',
                                   'charge', 'power', 'size', 'divergence', 'function', 'damping', 'spread',
                                   'bunch', 'separation', 'bucket', 'coupling', 'factor', 'magnetic', 'critical',
                                   'photon', 'revolution', 'ratio', 'electron', 'loss', 'turn', 'radiation',
                                   'lifetime', 'sector', 'cell', 'section', 'undulator', 'beam', 'horizontal',
                                   'vertical', 'momentum', 'compaction', 'chromaticity', 'synchrotron', 'wiggler',
                                   'alignment', 'tolerance', 'dipole', 'quadrupole', 'sextupole', 'bpm']
                    # Check if first header contains label words (e.g., "Electron energy", "Circumference", etc.)
                    if any(word in first_header.lower() for word in label_words):
                        is_label_value_headers = True
                    # Also check if second header looks like a value (numbers, units, etc.)
                    # Pattern: Second header is a value (number with unit, or just a number/unit)
                    if second_header and re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad|Hz|V|A|W|J|kg|g|s|min|h|d|y|°C|K|Pa|bar|atm|psi|N|kgf|lbf|m/s|km/h|mph|rpm|rad/s|deg/s)?\s*$', second_header, re.I):
                        is_label_value_headers = True
                    
                    
                
                if is_label_value_headers:
                    # Headers are actually label-value pairs - treat as data rows (no separate header row)
                    # GENERAL: When headers are label-value pairs, they should be treated as data, not headers
                    # Do NOT add a separator row - that would make them look like headers in markdown
                    
                    # Add headers as first data row (no separator - they're data, not headers)
                    if headers:
                        # GENERAL: Normalize header content - replace newlines with spaces
                        # Use unicode-aware regex to handle umlauts and special characters correctly
                        normalized_headers = []
                        for h in headers:
                            header_str = str(h)
                            header_str = re.sub(r'\s+', ' ', header_str, flags=re.UNICODE).strip()
                            normalized_headers.append(header_str)
                        markdown_output += "| " + " | ".join(normalized_headers) + " |\n"
                        # Skip first row if it matches headers (to avoid duplication)
                        if rows and len(rows) > 0:
                            first_row_str = "|".join(str(c).strip().lower() for c in rows[0][:len(headers)])
                            headers_str = "|".join(str(h).strip().lower() for h in headers)
                        
                        if first_row_str == headers_str:
                            rows = rows[1:]  # Skip duplicate first row
                else:
                    # Normal headers - NOT label-value pairs
                    
                    if headers:
                        # GENERAL: Normalize header content - replace newlines with spaces
                        # Use unicode-aware regex to handle umlauts and special characters correctly
                        normalized_headers = []
                        for h in headers:
                            header_str = str(h)
                            header_str = re.sub(r'\s+', ' ', header_str, flags=re.UNICODE).strip()
                            normalized_headers.append(header_str)
                        markdown_output += "| " + " | ".join(normalized_headers) + " |\n"
                        markdown_output += "| " + " | ".join(["---"] * len(headers)) + " |\n"
            elif rows and len(rows[0]) > 1:
                # GENERAL: Only use first row as header if it doesn't contain data patterns
                # If first row contains email, phone, location, or other data patterns, treat all rows as data
                first_row = rows[0]
                first_row_text = " ".join(str(cell) for cell in first_row).lower()
                # Check if first row contains data patterns (email, phone, location, etc.)
                data_patterns = [
                    r'e-mail|email|mailto:',
                    r'phone|tel|telephone|\+\d',
                    r'location|address|office|room',
                    r'@\w+\.\w+',  # Email addresses
                ]
                has_data_patterns = any(re.search(pattern, first_row_text, re.I) for pattern in data_patterns)
                
                # Check if first row is a label-value pair (common in parameter tables)
                # Pattern: First column ends with ":" and second column is a value (number, unit, etc.)
                is_label_value_pair = False
                if len(first_row) >= 2:
                    first_cell = str(first_row[0]).strip()
                    second_cell = str(first_row[1]).strip()
                    # Check if first cell ends with ":" (label indicator)
                    if first_cell.endswith(':') or first_cell.endswith('：'):
                        is_label_value_pair = True
                    # Also check if first cell contains common label words
                    label_words = ['energy', 'circumference', 'number', 'length', 'angle', 'radius', 'field', 
                                   'aperture', 'gradient', 'emittance', 'tune', 'frequency', 'time', 'current',
                                   'charge', 'power', 'size', 'divergence', 'function', 'damping', 'spread']
                    if any(word in first_cell.lower() for word in label_words):
                        is_label_value_pair = True
                
                # GENERAL: Check if first row is a timeline/career entry (date/period in first column)
                # Pattern: First cell contains date/period pattern (years, "Seit", "Von", etc.) and second cell is long description
                is_timeline_entry = False
                if len(first_row) >= 2:
                    first_cell = str(first_row[0]).strip()
                    second_cell = str(first_row[1]).strip()
                    # Check if first cell contains date/period patterns
                    date_patterns = [
                        r'\d{4}',  # Year (e.g., "2015", "2007")
                        r'seit|von|bis|until|from|to',  # Period words (German/English)
                        r'\d{4}\s*[-–—]\s*\d{4}',  # Year range (e.g., "2007 - 2015")
                        r'\d{4}\s*–\s*\d{4}',  # Year range with en-dash
                    ]
                    has_date_pattern = any(re.search(pattern, first_cell, re.I) for pattern in date_patterns)
                    # If first cell has date pattern and second cell is a long description (>30 chars), it's likely a timeline entry
                    if has_date_pattern and len(second_cell) > 30:
                        is_timeline_entry = True
                
                # Also check if first row is very long (likely data, not header)
                # Headers are typically short labels, data rows are longer
                first_row_length = sum(len(str(cell)) for cell in first_row)
                is_likely_data = has_data_patterns or is_label_value_pair or is_timeline_entry or first_row_length > 100
                
                
                
                if not is_likely_data:
                    # Use first row as header if no headers
                    # GENERAL: Normalize header content - replace newlines with spaces
                    # Use unicode-aware regex to handle umlauts and special characters correctly
                    normalized_first_row = []
                    for cell in first_row:
                        cell_str = str(cell)
                        cell_str = re.sub(r'\s+', ' ', cell_str, flags=re.UNICODE).strip()
                        normalized_first_row.append(cell_str)
                    markdown_output += "| " + " | ".join(normalized_first_row) + " |\n"
                    markdown_output += "| " + " | ".join(["---"] * len(first_row)) + " |\n"
                    rows = rows[1:]
                # else: treat all rows as data (no header row)
            
            # Add data rows
            rows_added = 0
            for row in rows:
                row_data = row[:len(headers)] if headers and len(row) >= len(headers) else row
                # Skip empty/separator-only rows
                if not any(str(c).strip() for c in row_data):
                    continue
                if all(str(c).strip() in ['', '---', '—', '–'] for c in row_data):
                    continue
                
                # GENERAL: Filter malformed rows (too many columns, concatenated data)
                # Pattern: Row with 10+ columns is likely malformed (concatenated data)
                num_cols = len(row_data)
                if num_cols > 10:
                    
                    continue
                
                # Pattern: Only filter single-column rows with field labels, or rows where ALL cells have labels
                # Multi-column tables (2-10 columns) with field labels in cells are legitimate structured data
                if num_cols == 1:
                    # Single column: filter if has 3+ field labels (concatenated)
                    first_cell = str(row_data[0]).strip() if row_data else ""
                    if first_cell:
                        field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', first_cell, re.I))
                        if field_label_count >= 3:
                            
                            continue
                elif num_cols >= 2:
                    # Multi-column: only filter if ALL cells have field labels AND many columns (indicates concatenation)
                    # GENERAL: 2-4 column tables with structured data in cells (like staff tables) are legitimate
                    # Only filter if 5+ columns AND all cells have labels (likely concatenated)
                    all_cells_have_labels = True
                    for cell in row_data:
                        cell_str = str(cell).strip()
                        if cell_str:
                            field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', cell_str, re.I))
                            if field_label_count == 0:
                                all_cells_have_labels = False
                                break
                    if all_cells_have_labels and num_cols >= 5:
                        # All cells have labels and 5+ columns = likely concatenated
                        
                        continue
                
                # GENERAL: Normalize cell content - replace newlines with spaces to prevent broken table rows
                # This fixes cases where names like "Anna\n\n\nBarinskaya" break table formatting
                # Also handle encoding issues with umlauts and special characters
                # FIX: If first cell contains a partial name (ends with umlaut) and second cell has a link with full name,
                # use the full name from the link instead
                normalized_cells = []
                for i, cell in enumerate(row_data):
                    cell_str = str(cell)
                    # Replace all newlines and multiple spaces with single space
                    # Use unicode-aware regex to handle umlauts and special characters correctly
                    cell_str = re.sub(r'\s+', ' ', cell_str, flags=re.UNICODE).strip()
                    
                    # FIX: If this is the first cell and it ends with an umlaut (ö, ü, ä), check if next cell has a link with full name
                    if i == 0 and cell_str and len(cell_str) > 2:
                        # Check if cell ends with umlaut (likely truncated name)
                        cell_str_stripped = cell_str.strip()
                        if cell_str_stripped and cell_str_stripped[-1] in ['ö', 'ü', 'ä', 'Ö', 'Ü', 'Ä']:
                            
                            
                            # Look for a link in the next few cells that might contain the full name
                            for j in range(1, min(4, len(row_data))):
                                next_cell = str(row_data[j])
                                # Extract name from markdown link pattern: [Name](mailto:...)
                                link_match = re.search(r'\[([^\]]+)\]\(mailto:', next_cell)
                                if link_match:
                                    full_name = link_match.group(1).strip()
                                    
                                    
                                    # Check if full_name starts with the partial name
                                    # Also check if they're similar (partial name is a prefix of full name)
                                    if (full_name.lower().startswith(cell_str_stripped.lower()) or 
                                        (len(cell_str_stripped) >= 3 and cell_str_stripped.lower() in full_name.lower() and 
                                         full_name.lower().index(cell_str_stripped.lower()) == 0)):
                                        # Use the full name instead
                                        cell_str = full_name
                                        
                                        break
                    
                    normalized_cells.append(cell_str)
                markdown_output += "| " + " | ".join(normalized_cells) + " |\n"
                rows_added += 1
            
            
            
            markdown_output += "\n"
        
        elif item['type'] == 'paragraph':
            # Add paragraph text with preserved links
            para_text = item.get('text', '')
            # FIX: Allow shorter paragraphs (15+ chars) - they may be questions or descriptive text after headings
            if para_text and len(para_text.strip()) >= 15:
                markdown_output += para_text + "\n\n"
        
        elif item['type'] == 'list':
            # Add list items with preserved links
            list_items = item.get('items', [])
            is_ordered = item.get('ordered', False)
            
            if list_items:
                for i, list_item in enumerate(list_items):
                    if is_ordered:
                        # Ordered list (1., 2., 3., ...)
                        markdown_output += f"{i + 1}. {list_item}\n"
                    else:
                        # Unordered list (*)
                        markdown_output += f"  * {list_item}\n"
                markdown_output += "\n"
        
        elif item['type'] == 'text':
            # Generic text content
            text = item.get('text', '')
            if text and len(text.strip()) >= 20:
                markdown_output += text + "\n\n"
    
    return markdown_output



def inject_links_into_markdown_tables(markdown_content, html_content):
    """
    Inject links directly into table sections in the markdown content.
    
    This function:
    1. Extracts all tables from HTML with links preserved
    2. Finds corresponding table sections in the markdown
    3. Replaces them in-place with enriched versions
    4. Uses content matching to ensure correct replacement without mixing tables
    
    NEW APPROACH: If markdown tables don't have email links but HTML does,
    replace ALL markdown tables with HTML-extracted versions.
    
    Args:
        markdown_content: The markdown content from Crawl4AI
        html_content: The original HTML content
        
    Returns:
        Markdown content with links injected into tables
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return markdown_content
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        # Only get top-level tables to avoid nested table confusion
        html_tables = [t for t in soup.find_all('table', recursive=True) if t.find_parent('table') is None]
        
        if not html_tables:
            return markdown_content
        
        # Check if HTML tables have email links
        html_has_emails = False
        html_email_count = 0
        for html_table in html_tables:
            mailto_links = html_table.find_all('a', href=lambda x: x and x.startswith('mailto:'))
            if mailto_links:
                html_has_emails = True
                html_email_count += len(mailto_links)
        
        # Check if markdown has email links (proper markdown format with email addresses)
        markdown_has_emails = bool(re.search(r'\[[^\]]+\]\(mailto:[^\s@]+@[^\s@]+\.[^\s)]+\)', markdown_content))
        
        # Debug output
        if html_has_emails:
            print(f"[DEBUG] HTML has {html_email_count} email link(s)")
        if markdown_has_emails:
            print(f"[DEBUG] Markdown already has email links")
        else:
            print(f"[DEBUG] Markdown does NOT have email links - will attempt injection")
        
        # Extract all tables from HTML with links preserved
        html_table_data = []
        used_tables = set()
        
        for html_table in html_tables:
            table_data = extract_table_from_html(html_table)
            # MINIMAL FIX: Only include tables that have rows (skip empty tables)
            if not table_data.get('rows'):
                continue
            # Get a unique identifier from the table (first few cells of first row)
            identifier = ""
            identifier_words = set()
            if table_data['rows']:
                first_row = table_data['rows'][0]
                identifier = " ".join(str(cell)[:30] for cell in first_row[:3] if cell)
                identifier_words = set(identifier.lower().split())
            
            # Also get a sample from headers if available
            if table_data['headers']:
                header_text = " ".join(str(h)[:20] for h in table_data['headers'][:3] if h)
                identifier_words.update(header_text.lower().split())
            
            html_table_data.append({
                'data': table_data,
                'identifier': identifier,
                'identifier_words': identifier_words,
                'formatted': format_table_markdown_inline(table_data)
            })
        
        # AGGRESSIVE MODE: If HTML has emails but markdown doesn't, replace ALL markdown tables with HTML tables
        # This is simpler and more reliable than trying to match individual tables
        if html_has_emails and not markdown_has_emails and html_table_data:
            print(f"[INFO] HTML has {len(html_table_data)} table(s) with emails, markdown has none - using aggressive replacement")
            # Find all table sections in markdown and replace them with HTML tables in order
            lines = markdown_content.split('\n')
            result_lines = []
            i = 0
            markdown_table_count = 0
            
            while i < len(lines):
                line = lines[i]
                
                # Check if this line looks like the start of a table row (contains |)
                if '|' in line and not line.strip().startswith('#'):
                    # Find the table section
                    table_start = i
                    table_end = i
                    
                    while table_end < len(lines):
                        current_line = lines[table_end]
                        if '|' in current_line:
                            table_end += 1
                        elif current_line.strip() == '':
                            if table_end + 1 < len(lines) and '|' in lines[table_end + 1]:
                                table_end += 1
                            else:
                                break
                        else:
                            break
                    
                    # Replace this markdown table with corresponding HTML table (if available)
                    if markdown_table_count < len(html_table_data):
                        result_lines.append(html_table_data[markdown_table_count]['formatted'])
                        used_tables.add(markdown_table_count)
                        markdown_table_count += 1
                        i = table_end
                        continue
                    else:
                        # More markdown tables than HTML tables, keep original
                        for j in range(table_start, table_end):
                            result_lines.append(lines[j])
                        i = table_end
                        continue
                
                # Not a table line, keep as-is
                result_lines.append(line)
                i += 1
            
            # FIX 4: Don't add "## Extracted Tables" here - DOM-order extraction handles tables separately
            # This prevents duplicate single-column tables from appearing before DOM-ordered tables
            # If no markdown tables found, DOM-order extraction will add tables with proper headings
            # So we skip adding tables here to avoid duplicates
            pass  # Removed: was adding single-column tables that duplicate DOM-order extraction
            
            return "\n".join(result_lines)
        
        # Find and replace table sections in markdown (original matching approach for when emails already exist)
        lines = markdown_content.split('\n')
        result_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Check if this line looks like the start of a table row (contains |)
            if '|' in line and not line.strip().startswith('#'):
                # Find the table section (consecutive lines with |)
                table_start = i
                table_end = i
                
                # Collect table rows - look for separator line (---)
                has_separator = False
                while table_end < len(lines):
                    current_line = lines[table_end]
                    if '|' in current_line:
                        # Check if it's a separator line
                        if re.match(r'^\s*\|[\s\-:]+\|', current_line):
                            has_separator = True
                        table_end += 1
                    elif current_line.strip() == '':
                        # Empty line - check if next line continues table
                        if table_end + 1 < len(lines) and '|' in lines[table_end + 1]:
                            table_end += 1
                        else:
                            break
                    else:
                        # Non-table line, end of table
                        break
                
                # Extract table content for matching
                table_lines = lines[table_start:table_end]
                table_text = "\n".join(table_lines)
                
                # CRITICAL: Check if table already has PROPER links (with email addresses, not just names)
                # Check if table has markdown links with actual email addresses in them
                # Pattern: [text](mailto:email@domain.com) - we want the email, not just the name
                # Also check for plain email addresses (might be in raw_markdown)
                has_proper_email_links = bool(re.search(r'\[[^\]]+\]\(mailto:[^\s@]+@[^\s@]+\.[^\s)]+\)', table_text))
                has_plain_emails = bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', table_text))
                has_proper_http_links = '](http' in table_text or '](https' in table_text
                
                # Only skip if table has proper links with actual URLs/emails
                # Don't skip if it just has text that looks like a name (might be link text without href)
                # BUT: If it has plain emails (not in markdown links), we should still process to convert them
                if (has_proper_email_links or has_proper_http_links) and not (has_plain_emails and not has_proper_email_links):
                    # Table already has proper links, just copy it as-is and skip processing
                    for j in range(table_start, table_end):
                        result_lines.append(lines[j])
                    i = table_end
                    continue
                
                # Table doesn't have proper links, proceed with matching and replacement
                table_words = set(table_text.lower().split())
                
                # Try to match with HTML tables (only unused ones)
                best_match = None
                best_match_score = 0
                best_match_idx = -1
                
                for idx, html_table_info in enumerate(html_table_data):
                    if idx in used_tables:
                        continue
                    
                    # Count matching words between table and HTML table
                    common_words = html_table_info['identifier_words'] & table_words
                    score = len(common_words)
                    
                    # Bonus points if identifier text appears in table
                    if html_table_info['identifier'] and html_table_info['identifier'].lower()[:50] in table_text.lower():
                        score += 5
                    
                    # Bonus for matching names (common in member tables)
                    # Check if any person names from HTML table appear in markdown table
                    identifier_lower = html_table_info['identifier'].lower()
                    for word in table_words:
                        if len(word) > 3 and word in identifier_lower:
                            score += 2
                    
                    if score > best_match_score and score > 0:
                        best_match_score = score
                        best_match = html_table_info
                        best_match_idx = idx
                
                # If we found a good match, replace the table section
                if best_match and best_match_score >= 1:  # Lowered threshold to 1 for better matching
                    # Replace table section with enriched version
                    result_lines.append(best_match['formatted'])
                    used_tables.add(best_match_idx)
                    i = table_end
                    continue
                else:
                    # No match found - try to extract emails from HTML directly as fallback
                    # This handles cases where table structure differs but emails are present
                    if BEAUTIFULSOUP_AVAILABLE:
                        try:
                            soup = BeautifulSoup(html_content, 'lxml')
                            html_tables_fallback = soup.find_all('table', recursive=True)
                            
                            # Try to find emails in cells that match the markdown table content
                            for html_table in html_tables_fallback:
                                html_text = html_table.get_text().lower()
                                table_text_lower = table_text.lower()
                                
                                # If there's some overlap in content, try to extract emails
                                common_words = set(html_text.split()) & set(table_text_lower.split())
                                if len(common_words) >= 2:  # At least 2 common words
                                    # Extract all mailto: links from this table
                                    mailto_links = html_table.find_all('a', href=lambda x: x and x.startswith('mailto:'))
                                    if mailto_links:
                                        # Found emails - try to inject them into the markdown table
                                        # Create a comprehensive email mapping from HTML
                                        email_map = {}
                                        name_to_email = {}
                                        
                                        for link in mailto_links:
                                            email = unescape(link.get('href', '')[7:])
                                            link_text = link.get_text(strip=True)
                                            
                                            # Map link text (name) to email
                                            if link_text:
                                                email_map[link_text.lower()] = email
                                                name_to_email[link_text.lower()] = email
                                            
                                            # Also try to find the person's name in the same row/cell
                                            # Look for parent cell/row to get context
                                            parent_cell = link.find_parent(['td', 'th'])
                                            if parent_cell:
                                                # Get all text from the row to find the person's name
                                                parent_row = parent_cell.find_parent('tr')
                                                if parent_row:
                                                    row_text = parent_row.get_text()
                                                    # Extract potential names (words that might be names)
                                                    # Look for patterns like "Name | E-Mail: [Name](mailto:email)"
                                                    cells = parent_row.find_all(['td', 'th'])
                                                    for cell in cells:
                                                        cell_text = cell.get_text(strip=True)
                                                        # If this cell contains the email link, check adjacent cells for name
                                                        if link in cell.find_all('a'):
                                                            # Check previous cells for name
                                                            cell_idx = cells.index(cell)
                                                            if cell_idx > 0:
                                                                prev_cell_text = cells[cell_idx - 1].get_text(strip=True)
                                                                if prev_cell_text and len(prev_cell_text) > 3:
                                                                    name_to_email[prev_cell_text.lower()] = email
                                        
                                        # Try to replace names with emails in the markdown table
                                        if email_map or name_to_email:
                                            enriched_lines = []
                                            for line in table_lines:
                                                enriched_line = line
                                                # Look for patterns: "E-Mail: | Name" and replace Name with email
                                                # Try multiple patterns
                                                for name, email in name_to_email.items():
                                                    # Pattern 1: | E-Mail: | Name |
                                                    pattern1 = rf'(\|\s*E-Mail:\s*\|\s*){re.escape(name)}(?=\s*\|)'
                                                    if re.search(pattern1, enriched_line, re.IGNORECASE):
                                                        enriched_line = re.sub(pattern1, rf'\1[{email}](mailto:{email})', enriched_line, flags=re.IGNORECASE)
                                                    
                                                    # Pattern 2: Name | E-Mail: | Name (if name appears twice)
                                                    pattern2 = rf'({re.escape(name)}.*?\|\s*E-Mail:\s*\|\s*){re.escape(name)}(?=\s*\|)'
                                                    if re.search(pattern2, enriched_line, re.IGNORECASE):
                                                        enriched_line = re.sub(pattern2, rf'\1[{email}](mailto:{email})', enriched_line, flags=re.IGNORECASE)
                                                
                                                enriched_lines.append(enriched_line)
                                            
                                            # If we made changes, use enriched version
                                            if enriched_lines != table_lines:
                                                result_lines.extend(enriched_lines)
                                                i = table_end
                                                continue
                        except Exception as e:
                            # Log error for debugging but continue
                            print(f"[DEBUG] Fallback email injection failed: {e}")
                            pass
                    
                    # No match found and fallback didn't work, keep original table
                    for j in range(table_start, table_end):
                        result_lines.append(lines[j])
                    i = table_end
                    continue
            
            # Not a table line, keep as-is
            result_lines.append(line)
            i += 1
        
        return "\n".join(result_lines)
    
    except Exception as e:
        print(f"[WARNING] Error injecting links into markdown: {e}")
        import traceback
        traceback.print_exc()
        return markdown_content


def format_table_markdown_inline(table):
    """
    Format a table dictionary as markdown (inline version, no extra headers).
    
    Args:
        table: Table dict with headers, rows, and caption
        
    Returns:
        Markdown string with table formatted
    """
    markdown = ""
    
    caption = table.get('caption', '')
    if caption:
        markdown += f"*{caption}*\n\n"
    
    headers = table.get('headers', [])
    rows = table.get('rows', [])

    # Minimal key-value handling (Issue #3 / #5):
    # Some 2-column tables are actually field|value pairs, but our header extraction can
    # mistakenly treat the first pair as "headers", creating a separator row like:
    # | Electron energy | 6.0 GeV |
    # | --- | --- |
    #
    # Heuristic (simple, structural): if there are exactly 2 "headers" and the remaining
    # rows are consistently 2 columns, treat headers as the first data row.
    is_key_value_table = False
    if headers and len(headers) == 2 and rows and all(len(r) == 2 for r in rows[:5]):
        h0 = str(headers[0] or "").strip()
        h1 = str(headers[1] or "").strip()
        # Treat empty extracted "headers" as a key-value table indicator
        if not h0 and not h1:
            is_key_value_table = True
        # header looks like "Label:" or "Value" (has digits) → likely not a real column header
        if h0.endswith(':') or h1.endswith(':') or (re.search(r'\d', h1) is not None):
            is_key_value_table = True
    
    # Only add header row if we have explicit headers AND it's not key-value
    if headers and rows and not is_key_value_table:
        # We have explicit headers - format as standard markdown table
        markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
        markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in rows:
            # Ensure row has same number of cells as headers
            row_data = row[:len(headers)] if len(row) >= len(headers) else row + [''] * (len(headers) - len(row))
            # Skip empty/separator-only rows
            if not any(str(cell).strip() for cell in row_data):
                continue
            if all(str(cell).strip() in ['', '---', '—', '–'] for cell in row_data):
                continue
            # Clean each cell: remove excessive whitespace, handle empty cells
            cleaned_row = []
            for cell in row_data:
                cell_str = str(cell).strip()
                # Only remove truly duplicate words that are clearly errors (not names or before links)
                # Skip if the duplicate word is followed by a link (likely a name)
                cell_str = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_str)  # Remove duplicate words, but not before links
                cleaned_row.append(cell_str if cell_str else '')
            markdown += "| " + " | ".join(cleaned_row) + " |\n"
    elif rows:
        # No headers OR key-value: format without a header row
        # Determine max columns from all rows
        if is_key_value_table:
            # Treat "headers" as the first data row
            rows = [headers] + rows
            headers = []
        max_cols = max(len(row) for row in rows) if rows else 0
        for row in rows:
            # Pad row to max_cols if needed
            row_data = row + [''] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols]
            # Skip empty/separator-only rows
            if not any(str(cell).strip() for cell in row_data):
                continue
            if all(str(cell).strip() in ['', '---', '—', '–'] for cell in row_data):
                continue
            # Clean each cell
            cleaned_row = []
            for cell in row_data:
                cell_str = str(cell).strip()
                # Only remove truly duplicate words that are clearly errors (not names or before links)
                # Skip if the duplicate word is followed by a link (likely a name)
                cell_str = re.sub(r'\b([A-Z][a-z]+)\s+\1\b(?!\s*\[)', r'\1', cell_str)  # Remove duplicate words, but not before links
                cleaned_row.append(cell_str if cell_str else '')
            markdown += "| " + " | ".join(cleaned_row) + " |\n"
    
    return markdown


def get_table_header_normalized(formatted_table):
    """
    Extract and normalize the header from a formatted markdown table.
    Returns normalized header string, or None if no header found.
    """
    if _is_empty_or_whitespace(formatted_table):
        return None
    lines = formatted_table.split('\n')
    for line in lines:
        if line.strip() and '|' in line and not re.match(r'^\s*\|[\s\-:]+\|', line):
            return re.sub(r'\s+', ' ', line.lower().strip())
    return None



