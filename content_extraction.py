"""
Content extraction module for the DESY web crawler.

Extracted in refactoring Step 4 from crawl_desy_all_urls.py.
Contains: Indico event extraction, external link extraction, contact block
extraction, deduplication / noise removal, and markdown post-processing.
"""

import re
import hashlib
from pathlib import Path
from urllib.parse import urlparse, urljoin

# URL utilities (extracted in Step 2)
import url_utils as _url_utils

# Table processing (extracted in Step 3)
import table_processing as _table_processing

# BeautifulSoup for HTML parsing
try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False

# Content hash for deduplication
try:
    from crawl4ai.content_hash import ContentHash
    CONTENT_HASH_AVAILABLE = True
except ImportError:
    try:
        from crawl4ai.content_comparison import ContentHash
        CONTENT_HASH_AVAILABLE = True
    except ImportError:
        CONTENT_HASH_AVAILABLE = False


# ============================================================================
# PERFORMANCE FIX: Soup caching to avoid redundant HTML parsing
# ============================================================================
# Parsing large HTML documents with BeautifulSoup is expensive (50-200ms per page).
# The same result.html is often parsed 4-5 times in different code paths.
# This cache ensures each HTML is parsed only once per result.

_soup_cache = {}  # Keyed by result URL

def _get_cached_soup(result):
    """
    Get a cached BeautifulSoup object for a crawl result's HTML.

    PERFORMANCE: Avoids redundant parsing of the same HTML multiple times.
    Each parse of a 200KB HTML takes ~100ms; with 4-5 parses per page at 5000 pages,
    this saves ~2000+ seconds per crawl.

    Args:
        result: Crawl result object with .url and .html attributes

    Returns:
        BeautifulSoup object, or None if HTML not available
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return None
    if not result or not hasattr(result, 'html') or not result.html:
        return None

    url = getattr(result, 'url', None)
    if not url:
        # No URL to cache by - parse directly
        return BeautifulSoup(result.html, 'lxml')

    if url not in _soup_cache:
        _soup_cache[url] = BeautifulSoup(result.html, 'lxml')

    return _soup_cache[url]


def _clear_soup_cache():
    """Clear the soup cache to free memory after processing."""
    _soup_cache.clear()


# ============================================================================
# Link Visibility Detection (JavaScript)
# ============================================================================

def get_link_visibility_detection_script():
    """
    Returns JavaScript code that detects which links are visible and clickable.
    This should be executed in the browser AFTER page load to identify inactive links.
    
    Returns:
        str: JavaScript code that returns an array of invisible link URLs
    """
    return """
    (function() {
        const invisibleLinks = [];
        const allLinks = document.querySelectorAll('a[href]');
        
        for (const link of allLinks) {
            const href = link.getAttribute('href');
            if (!href || href.startsWith('#')) continue;  // Skip anchors and empty hrefs
            
            let isVisible = true;
            let reason = null;
            
            // Check 1: Element has display:none or visibility:hidden
            const computedStyle = window.getComputedStyle(link);
            if (computedStyle.display === 'none') {
                isVisible = false;
                reason = 'display:none';
            } else if (computedStyle.visibility === 'hidden') {
                isVisible = false;
                reason = 'visibility:hidden';
            } else if (computedStyle.opacity === '0') {
                isVisible = false;
                reason = 'opacity:0';
            }
            
            // Check 2: Element or parent is disabled/inactive
            if (isVisible) {
                if (link.hasAttribute('disabled') || 
                    link.hasAttribute('aria-disabled') ||
                    link.getAttribute('aria-disabled') === 'true') {
                    isVisible = false;
                    reason = 'disabled attribute or aria-disabled';
                }
                
                // Check if parent or ancestor is disabled/inactive
                let parent = link.parentElement;
                for (let i = 0; i < 5; i++) {  // Check up to 5 levels up
                    if (!parent) break;
                    const classes = parent.className || '';
                    const id = parent.id || '';
                    if (classes.includes('disabled') || 
                        classes.includes('inactive') || 
                        classes.includes('is-disabled') ||
                        id.includes('disabled') || 
                        id.includes('inactive')) {
                        isVisible = false;
                        reason = 'parent has disabled/inactive class';
                        break;
                    }
                    parent = parent.parentElement;
                }
            }
            
            // Check 3: Element is not in tab order (tabindex="-1" often means inactive)
            if (isVisible && link.hasAttribute('tabindex') && 
                link.getAttribute('tabindex') === '-1') {
                isVisible = false;
                reason = 'tabindex=-1 (not in tab order)';
            }
            
            // Check 4: Element is clipped or has zero dimensions
            if (isVisible) {
                const rect = link.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) {
                    isVisible = false;
                    reason = 'zero dimensions (width or height)';
                }
            }
            
            // Check 5: Element is off-screen (common for skip links)
            if (isVisible) {
                const rect = link.getBoundingClientRect();
                if (rect.top < -1000 || rect.left < -1000) {
                    isVisible = false;
                    reason = 'off-screen (skip link)';
                }
            }
            
            // If not visible, add to invisible links
            if (!isVisible) {
                invisibleLinks.push({
                    url: href,
                    text: link.textContent.trim().substring(0, 50),
                    reason: reason
                });
            }
        }
        
        return invisibleLinks;
    })();
    """


# ============================================================================
# Indico Event Page Extractor
# ============================================================================
# Indico pages (indico.desy.de) have a specific structure for events/meetings.
# This extractor captures: event name, date, location, zoom links, contributions.

def is_indico_url(url):
    """Check if URL is an Indico event page."""
    if not url:
        return False
    return 'indico.desy.de' in url.lower() and '/event/' in url.lower()


def extract_indico_event(html_content, url=None):
    """
    Extract structured event information from Indico pages.
    
    Extracts:
    - Event title
    - Date and time
    - Location (room) or Zoom link
    - Description
    - Registration/submission deadlines
    - Contributions with speakers and attachments
    
    Args:
        html_content: Raw HTML from the Indico page
        url: The page URL (for reference)
        
    Returns:
        Markdown-formatted string with event info, or None if extraction fails
    """
    if not BEAUTIFULSOUP_AVAILABLE or not html_content:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        lines = []
        
        # === EVENT TITLE ===
        # Indico uses h1 with class "event-header-title" or similar
        title_elem = soup.find('h1', class_=lambda c: c and 'title' in str(c).lower())
        if not title_elem:
            title_elem = soup.find('h1')
        if title_elem:
            title = title_elem.get_text(strip=True)
            lines.append(f"# {title}")
            lines.append("")
        
        # === DATE AND TIME ===
        # Look for date/time elements
        date_elem = soup.find(['time', 'span', 'div'], class_=lambda c: c and ('date' in str(c).lower() or 'time' in str(c).lower()))
        if not date_elem:
            # Try finding by datetime attribute
            date_elem = soup.find('time', attrs={'datetime': True})
        if not date_elem:
            # Try common patterns in text
            for elem in soup.find_all(['span', 'div', 'p']):
                text = elem.get_text(strip=True)
                # Look for date patterns like "Friday Jan 16, 2026"
                if re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+\w+\s+\d+', text, re.I):
                    date_elem = elem
                    break
        
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            # Clean up the date text
            date_text = re.sub(r'\s+', ' ', date_text)
            lines.append(f"**Date:** {date_text}")
            lines.append("")
        
        # === LOCATION ===
        # Look for location/room info
        location_elem = soup.find(['span', 'div'], class_=lambda c: c and 'location' in str(c).lower())
        if not location_elem:
            # Try looking for room patterns
            for elem in soup.find_all(['span', 'div', 'p']):
                text = elem.get_text(strip=True)
                # Room patterns like "125 (68)" or "Room 125"
                if re.match(r'^\d+\s*\(\d+\)$', text) or 'room' in text.lower():
                    location_elem = elem
                    break
        
        if location_elem:
            location = location_elem.get_text(strip=True)
            lines.append(f"**Location:** {location}")
            lines.append("")
        
        # === ZOOM/VIDEO LINK ===
        # Extract Zoom or video conference links
        zoom_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(vc in href.lower() for vc in ['zoom.us', 'teams.microsoft', 'meet.google', 'webex', 'bluejeans']):
                link_text = link.get_text(strip=True) or href
                zoom_links.append(f"[{link_text}]({href})")
        
        if zoom_links:
            lines.append("**Video Conference:**")
            for zl in zoom_links:
                lines.append(f"- {zl}")
            lines.append("")
        
        # === DESCRIPTION ===
        # Look for description/abstract section
        desc_elem = soup.find(['div', 'section'], class_=lambda c: c and 'description' in str(c).lower())
        if not desc_elem:
            desc_elem = soup.find(['div', 'section'], id=lambda i: i and 'description' in str(i).lower())
        
        if desc_elem:
            desc_text = desc_elem.get_text(separator=' ', strip=True)
            # Clean up and limit length
            desc_text = re.sub(r'\s+', ' ', desc_text)
            if desc_text and len(desc_text) > 10:
                lines.append("**Description:**")
                lines.append(desc_text[:1000])  # Limit to 1000 chars
                lines.append("")
        
        # === DEADLINES ===
        # Look for registration/submission deadlines
        deadline_patterns = ['deadline', 'registration', 'submission', 'abstract']
        for pattern in deadline_patterns:
            for elem in soup.find_all(['div', 'span', 'p', 'dt', 'dd']):
                text = elem.get_text(strip=True).lower()
                if pattern in text and ('deadline' in text or re.search(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', text)):
                    full_text = elem.get_text(strip=True)
                    if len(full_text) < 200:  # Reasonable length for a deadline
                        lines.append(f"**{full_text}**")
        
        if any('deadline' in l.lower() for l in lines):
            lines.append("")
        
        # === CONTRIBUTIONS/AGENDA ===
        # Look for timetable/contributions
        contributions = []
        
        # Find timetable entries (usually in a structured list or table)
        timetable = soup.find(['div', 'section', 'ul'], class_=lambda c: c and ('timetable' in str(c).lower() or 'contributions' in str(c).lower() or 'agenda' in str(c).lower()))
        
        if not timetable:
            # Try finding by common patterns
            timetable = soup.find(['div', 'section'], id=lambda i: i and ('timetable' in str(i).lower() or 'schedule' in str(i).lower()))
        
        if timetable:
            # Look for individual entries
            entries = timetable.find_all(['div', 'li', 'tr'], class_=lambda c: c and ('entry' in str(c).lower() or 'contribution' in str(c).lower() or 'talk' in str(c).lower()))
            
            for entry in entries[:20]:  # Limit to 20 contributions
                entry_text = entry.get_text(separator=' ', strip=True)
                entry_text = re.sub(r'\s+', ' ', entry_text)
                
                # Extract time if present
                time_match = re.search(r'\d{1,2}:\d{2}\s*(AM|PM|am|pm)?', entry_text)
                time_str = time_match.group(0) if time_match else ""
                
                # Extract speaker name
                speaker_elem = entry.find(['span', 'div'], class_=lambda c: c and 'speaker' in str(c).lower())
                speaker = speaker_elem.get_text(strip=True) if speaker_elem else ""
                
                # Extract title
                title_elem = entry.find(['span', 'div', 'a'], class_=lambda c: c and 'title' in str(c).lower())
                contrib_title = title_elem.get_text(strip=True) if title_elem else ""
                
                # Extract attachment links (PDFs, etc.)
                attachments = []
                for link in entry.find_all('a', href=True):
                    href = link.get('href', '')
                    if any(ext in href.lower() for ext in ['.pdf', '.pptx', '.ppt', '.doc', '/attachments/', '/material/']):
                        link_text = link.get_text(strip=True) or 'Attachment'
                        # Make absolute URL if needed
                        if not href.startswith('http'):
                            href = f"https://indico.desy.de{href}" if href.startswith('/') else href
                        attachments.append(f"[{link_text}]({href})")
                
                if time_str or contrib_title or speaker:
                    contrib_line = ""
                    if time_str:
                        contrib_line += f"**{time_str}** "
                    if contrib_title:
                        contrib_line += f"- {contrib_title}"
                    if speaker:
                        contrib_line += f" (Speaker: {speaker})"
                    contributions.append(contrib_line)
                    
                    for att in attachments:
                        contributions.append(f"  - {att}")
        
        # Also find standalone attachment links
        all_attachments = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.pdf', '.pptx', '.ppt', '.docx']):
                link_text = link.get_text(strip=True)
                if link_text and len(link_text) < 100:  # Reasonable filename length
                    if not href.startswith('http'):
                        href = f"https://indico.desy.de{href}" if href.startswith('/') else href
                    all_attachments.append(f"[{link_text}]({href})")
        
        if contributions:
            lines.append("## Agenda/Contributions")
            lines.append("")
            for c in contributions:
                lines.append(c)
            lines.append("")
        elif all_attachments:
            lines.append("## Attachments")
            lines.append("")
            for att in all_attachments:
                lines.append(f"- {att}")
            lines.append("")
        
        # === EXTERNAL LINKS ===
        # Collect important external links
        external_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            # Skip internal Indico links and very short text
            if href.startswith('http') and 'indico.desy.de' not in href and len(text) > 2:
                if href not in [l[1] for l in external_links]:  # Avoid duplicates
                    external_links.append((text, href))
        
        if external_links:
            lines.append("## External Links")
            lines.append("")
            for text, href in external_links[:10]:  # Limit to 10 links
                lines.append(f"- [{text}]({href})")
            lines.append("")
        
        if lines:
            result = '\n'.join(lines)
            
            # === CLEANUP: Remove browser warning and duplicate content ===
            # Filter out "browser out of date" warning that Indico shows
            result = re.sub(r'#{1,6}\s*⚠\s*Your browser is out of date\s*⚠.*?Indico may not work correctly in this browser\.?\s*\n?', '', result, flags=re.IGNORECASE | re.DOTALL)
            result = re.sub(r'⚠\s*Your browser is out of date\s*⚠.*?Indico may not work correctly in this browser\.?\s*\n?', '', result, flags=re.IGNORECASE | re.DOTALL)
            
            # Remove duplicate External Links sections (keep only the first one)
            external_links_pattern = r'(## External Links\n\n(?:- \[[^\]]+\]\([^)]+\)\n)+\n)'
            matches = list(re.finditer(external_links_pattern, result))
            if len(matches) > 1:
                # Keep only the first External Links section
                for m in matches[1:]:
                    result = result[:m.start()] + result[m.end():]
            
            # Remove excessive newlines
            result = re.sub(r'\n{4,}', '\n\n\n', result)
            
            return result.strip()
        
    except Exception as e:
        print(f"[WARNING] Indico extraction failed: {e}")
    
    return None


# ============================================================================
# External Link Extraction
# ============================================================================

def extract_external_links(html_content, current_url):
    """
    Extract all external links (links to different domains) with their text and associated headings.
    
    IMPORTANT: Links inside tables are SKIPPED - they should remain in their table positions.
    Only links outside tables are extracted here, grouped by their associated headings/sections.
    
    Args:
        html_content: HTML content as string
        current_url: Current page URL to determine external links
        
    Returns:
        Markdown string with external links grouped by section/heading, or empty string if none found
    """
    if not BEAUTIFULSOUP_AVAILABLE or not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        current_domain = _url_utils._normalize_domain(urlparse(current_url).netloc)
        
        # Find all links
        links = soup.find_all('a', href=True)
        external_links_by_section = {}  # Dict: section_heading -> list of links
        seen_links = set()  # Deduplicate by URL
        
        for link in links:
            href = link.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('mailto:'):
                continue
            
            # SKIP links inside tables - they should stay in their table positions
            # Check if link is inside a table cell (td/th) or directly inside a table
            parent_table = link.find_parent('table')
            parent_cell = link.find_parent(['td', 'th'])
            
            # Link is in a table if it's inside a table element OR inside a table cell
            if parent_table or parent_cell:
                continue
            
            # Make absolute URL
            absolute_url = urljoin(current_url, href)
            parsed = urlparse(absolute_url)
            link_domain = _url_utils._normalize_domain(parsed.netloc)
            
            # Check if external (different domain)
            if link_domain and link_domain != current_domain:
                # Skip if already seen
                if absolute_url in seen_links:
                    continue
                seen_links.add(absolute_url)
                
                # Get link text
                link_text = link.get_text(strip=True)
                if not link_text:
                    link_text = absolute_url
                
                # Find associated heading/section for this link
                # Look for nearest heading (h1-h6) before this link in the DOM
                section_heading = None
                best_heading = None
                best_heading_level = 7  # Start with level higher than any real heading
                
                # Strategy: Find all headings before this link in document order
                # Then pick the closest one (highest level, most recent)
                link_position = None
                try:
                    # Get all elements before this link
                    all_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'])
                    for i, elem in enumerate(all_elements):
                        if elem == link:
                            link_position = i
                            break
                    
                    if link_position is not None:
                        # Find all headings before this link
                        for i in range(link_position - 1, -1, -1):
                            elem = all_elements[i]
                            if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                heading_level_num = int(elem.name[1])
                                heading_text = elem.get_text(strip=True)
                                if heading_text and heading_level_num < best_heading_level:
                                    best_heading = heading_text
                                    best_heading_level = heading_level_num
                                    # Prefer closer headings (stop if we found a good one)
                                    if heading_level_num <= 3:  # h1, h2, h3 are usually section headers
                                        break
                except Exception:
                    # Fallback: simple parent traversal
                    current = link
                    for _ in range(5):  # Limit search depth
                        if current is None:
                            break
                        # Check previous siblings
                        prev = current.find_previous_sibling()
                        while prev:
                            if prev.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                heading_level_num = int(prev.name[1])
                                heading_text = prev.get_text(strip=True)
                                if heading_text and heading_level_num < best_heading_level:
                                    best_heading = heading_text
                                    best_heading_level = heading_level_num
                            prev = prev.find_previous_sibling()
                        current = current.parent
                
                section_heading = best_heading
                
                # Use section heading or default
                section_key = section_heading if section_heading else "External Links"
                
                if section_key not in external_links_by_section:
                    external_links_by_section[section_key] = []
                
                external_links_by_section[section_key].append({
                    'text': link_text,
                    'url': absolute_url
                })
        
        # Format as markdown sections grouped by heading
        if external_links_by_section:
            markdown = ""
            # Sort sections: "External Links" last, others alphabetically
            sorted_sections = sorted(
                [k for k in external_links_by_section.keys() if k != "External Links"]
            )
            if "External Links" in external_links_by_section:
                sorted_sections.append("External Links")
            
            for section in sorted_sections:
                links_list = external_links_by_section[section]
                if links_list:
                    # Use the section heading as markdown heading (or default)
                    if section == "External Links":
                        markdown += "\n\n## External Links\n\n"
                    else:
                        # Use the section heading as-is (it's already a heading from HTML)
                        markdown += f"\n\n## {section}\n\n"
                    
                    for link in links_list:
                        markdown += f"- [{link['text']}]({link['url']})\n"
            
            return markdown
        
        return ""
    except Exception as e:
        # Silently fail - external links are optional
        return ""


# ============================================================================
# Enhanced Duplication Detection and Noise Removal Functions
# ============================================================================

def normalize_text_enhanced(text):
    """
    Enhanced normalization with word deduplication and markdown link normalization.
    
    This handles:
    - Markdown link whitespace normalization
    - Word-level deduplication ("Contact Contact" -> "Contact")
    - Standard text normalization
    """
    # First normalize markdown links (remove whitespace in link syntax)
    text = normalize_markdown_links(text)
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove markdown syntax for comparison (extract content)
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)  # Extract link text
    text = re.sub(r'mailto:\s*', '', text)  # Remove mailto: prefix
    
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove consecutive duplicate words
    words = text.split()
    deduplicated_words = []
    prev_word = None
    for word in words:
        if word != prev_word:
            deduplicated_words.append(word)
        prev_word = word
    
    return ' '.join(deduplicated_words)


def normalize_markdown_links(text):
    """
    Remove whitespace from markdown link syntax.
    
    Fixes:
    - [ text](url) -> [text](url)
    - (mailto: email) -> (mailto:email)
    - [email ](mailto: email ) -> [email](mailto:email)
    """
    # Remove spaces in link brackets
    text = re.sub(r'\[\s+', '[', text)
    text = re.sub(r'\s+\]', ']', text)
    
    # Remove spaces in link parentheses
    text = re.sub(r'\(\s+', '(', text)
    text = re.sub(r'\s+\)', ')', text)
    
    # Remove spaces after colons in URLs/mailto
    text = re.sub(r':\s+', ':', text)
    
    # Remove spaces before/after email addresses in links
    text = re.sub(r'\[(\s+)([^\]]+)(\s+)\]', r'[\2]', text)
    
    return text


def extract_emails_from_text(text):
    """Extract email addresses from text for deduplication."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    return [e.lower().strip() for e in emails]


def text_similarity(text1, text2):
    """Calculate similarity ratio between two texts (0.0 to 1.0)."""
    if not text1 or not text2:
        return 0.0
    
    # Use simple character-based similarity (can be enhanced with difflib)
    if text1 == text2:
        return 1.0
    
    # Calculate longest common subsequence ratio
    len1, len2 = len(text1), len(text2)
    if len1 == 0 or len2 == 0:
        return 0.0
    
    # Simple ratio: count matching characters
    matches = sum(1 for c1, c2 in zip(text1, text2) if c1 == c2)
    max_len = max(len1, len2)
    
    return matches / max_len if max_len > 0 else 0.0


def detect_enhanced_repetition(markdown_lines):
    """
    Enhanced repetition detection with:
    - Multi-line block comparison
    - Email address extraction and deduplication
    - Substring/containment detection
    - Paragraph extraction and comparison
    """
    duplicates = set()
    
    # 1. Email address deduplication (highest priority)
    email_to_lines = {}
    for i, line in enumerate(markdown_lines):
        emails = extract_emails_from_text(line)
        for email in emails:
            if email in email_to_lines:
                # This email was seen before - mark all occurrences as duplicates
                duplicates.update(email_to_lines[email])
                duplicates.add(i)
            else:
                email_to_lines[email] = []
            email_to_lines[email].append(i)
    
    # 2. Multi-line block detection (sliding window)
    seen_blocks = {}
    block_size = 3
    for i in range(len(markdown_lines) - block_size + 1):
        block = '\n'.join(markdown_lines[i:i+block_size])
        normalized = normalize_text_enhanced(block)
        
        if len(normalized) < 50:  # Skip very short blocks
            continue
        
        # Check similarity to seen blocks
        for seen_block, seen_indices in seen_blocks.items():
            similarity = text_similarity(normalized, seen_block)
            if similarity > 0.90:  # 90% similar
                duplicates.update(seen_indices)
                duplicates.update(range(i, i+block_size))
                break
        
        # Store this block
        if normalized not in seen_blocks:
            seen_blocks[normalized] = []
        seen_blocks[normalized].append(range(i, i+block_size))
    
    # 3. Single-line comparison with enhanced normalization
    seen_lines = {}
    for i, line in enumerate(markdown_lines):
        normalized = normalize_text_enhanced(line)
        
        # Skip very short lines (< 10 chars)
        if len(normalized) < 10:
            continue
        
        # Check similarity to seen lines
        for seen_line, seen_indices in seen_lines.items():
            similarity = text_similarity(normalized, seen_line)
            if similarity > 0.95:
                duplicates.update(seen_indices)
                duplicates.add(i)
                break
        
        # Check substring/containment relationships
        for seen_line, seen_indices in seen_lines.items():
            if normalized in seen_line or seen_line in normalized:
                # Keep the longer line, mark shorter as duplicate
                if len(normalized) < len(seen_line):
                    duplicates.add(i)
                else:
                    duplicates.update(seen_indices)
                break
        
        # Store this line
        if normalized not in seen_lines:
            seen_lines[normalized] = []
        seen_lines[normalized].append(i)
    
    # 4. Paragraph extraction and comparison
    paragraph_to_lines = {}
    paragraph_pattern = r'([^.!?]+[.!?])'
    
    for i, line in enumerate(markdown_lines):
        paragraphs = re.findall(paragraph_pattern, line)
        for para in paragraphs:
            normalized = normalize_text_enhanced(para)
            if len(normalized) < 30:  # Skip very short paragraphs
                continue
            
            if normalized in paragraph_to_lines:
                # This paragraph was seen before
                duplicates.update(paragraph_to_lines[normalized])
                duplicates.add(i)
            else:
                paragraph_to_lines[normalized] = []
            paragraph_to_lines[normalized].append(i)
    
    return duplicates


# ============================================================================
# Contact Block Extraction
# ============================================================================

def extract_contact_blocks(html_soup):
    """
    Extract complete contact blocks (name + title + phone + email + location).
    
    Strategy:
    1. Find elements containing email addresses
    2. Expand to parent container (paragraph, div, list item)
    3. Extract all text from container (preserves relationships)
    4. Group by proximity (same container = same person)
    """
    contact_blocks = []
    
    if not BEAUTIFULSOUP_AVAILABLE:
        return contact_blocks
    
    # Find all email links
    email_links = html_soup.find_all('a', href=re.compile(r'mailto:'))
    
    for link in email_links:
        # Extract email
        email = link.get('href', '').replace('mailto:', '').strip()
        if not email:
            continue
        
        # Find parent container (p, div, li, td, tr)
        # Try multiple parent levels to find the best container
        # Strategy: Find the smallest container that includes both name and email
        parent = None
        best_parent = None
        best_score = 0
        
        for parent_tag in ['p', 'div', 'li', 'td', 'tr', 'section', 'article']:
            candidate = link.find_parent(parent_tag)
            if candidate:
                candidate_text = candidate.get_text(strip=True)
                # Score based on:
                # 1. Has name-like pattern (2-4 capitalized words) - high priority
                # 2. Has substantial content (20+ chars) - medium priority
                # 3. Has phone pattern - bonus
                score = 0
                has_name_pattern = bool(re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', candidate_text))
                has_phone = bool(re.search(r'T\.\s*\(?\d+', candidate_text))
                
                if has_name_pattern:
                    score += 10
                if len(candidate_text) > 20:
                    score += 5
                if has_phone:
                    score += 2
                
                # Prefer containers with names and substantial content
                if score > best_score:
                    best_score = score
                    best_parent = candidate
                
                # Also keep first substantial candidate as fallback
                if not parent and len(candidate_text) > 20:
                    parent = candidate
        
        # Use best parent if found, otherwise use fallback
        parent = best_parent if best_parent else parent
        
        if parent:
            # Extract all text from parent (preserves structure)
            # Use separator=' ' to keep words together, but preserve line structure where possible
            contact_text = parent.get_text(separator=' ', strip=True)
            
            # Also try to get text with line breaks to preserve structure better
            # This helps when contact info spans multiple lines
            contact_text_with_breaks = parent.get_text(separator='\n', strip=True)
            # If text with breaks is longer, it might have better structure
            if len(contact_text_with_breaks) > len(contact_text) * 1.2:
                # Use the version with breaks, but normalize
                contact_text = re.sub(r'\n+', ' ', contact_text_with_breaks)
            
            # If parent doesn't have enough content or doesn't have a name pattern, 
            # try to expand to include siblings or parent's parent
            # This helps when name is in a previous sibling element or parent container
            if len(contact_text) < 30 or not re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', contact_text):
                # Try to find previous sibling that might contain the name
                prev_sibling = parent.find_previous_sibling()
                if prev_sibling:
                    prev_text = prev_sibling.get_text(separator=' ', strip=True)
                    # Check if it looks like a name (2-4 capitalized words, no title keywords)
                    if re.match(r'^[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', prev_text):
                        # Make sure it's not a title
                        title_keywords_check = ['team', 'head', 'manager', 'leader', 'trainer', 'assistant', 'hr', 'recruitment', 
                                              'employer', 'branding', 'scientist', 'technician', 'clerk', 'assistent', 'staff', 'scientific']
                        if not any(keyword in prev_text.lower() for keyword in title_keywords_check):
                            contact_text = prev_text + ' ' + contact_text
                
                # Also try parent's parent if current parent is too small
                if len(contact_text) < 50:
                    grandparent = parent.find_parent(['div', 'section', 'article', 'li'])
                    if grandparent and grandparent != parent:
                        grandparent_text = grandparent.get_text(separator=' ', strip=True)
                        # If grandparent has more content and includes a name pattern, use it
                        if len(grandparent_text) > len(contact_text) and re.search(r'\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+', grandparent_text):
                            contact_text = grandparent_text
            
            # Extract name (pattern: First Last or Last, First)
            # More flexible pattern: allows for middle names, titles (Dr., Prof.), and handles various formats
            # Allow special characters (umlauts) in names: Ä, Ö, Ü, ä, ö, ü, ß
            # Pattern 1: Standard name (First Last, First Middle Last, etc.) - with umlauts
            name_pattern1 = r'\b([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+)\b'
            # Pattern 2: Name with title prefix (Dr. John Smith, Prof. Dr. Jane Doe)
            name_pattern2 = r'\b(?:Dr\.|Prof\.|Prof\.\s+Dr\.)\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+)\b'
            # Pattern 3: Name at start of text (common in contact blocks) - with umlauts
            name_pattern3 = r'^([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)'
            
            names = []
            # Try all patterns
            names.extend(re.findall(name_pattern1, contact_text))
            names.extend(re.findall(name_pattern2, contact_text))
            names.extend(re.findall(name_pattern3, contact_text))
            
            # Remove duplicates while preserving order
            seen = set()
            unique_names = []
            for name in names:
                name_clean = name.strip()
                if name_clean and name_clean not in seen and len(name_clean) > 3:
                    seen.add(name_clean)
                    unique_names.append(name_clean)
            
            names = unique_names
            
            # Filter out names that are actually titles (common false positives)
            # Titles often look like names: "Team Leader", "Head of", etc.
            title_keywords = ['team', 'head', 'manager', 'leader', 'trainer', 'assistant', 'hr', 'recruitment', 
                            'employer', 'branding', 'scientist', 'technician', 'clerk', 'assistent', 'staff', 'scientific']
            filtered_names = []
            for name in names:
                name_lower = name.lower()
                # Skip if name contains title keywords (likely a title, not a name)
                if not any(keyword in name_lower for keyword in title_keywords):
                    # Also check if it's a reasonable name length (2-4 words, each capitalized)
                    words = name.split()
                    # Allow names with special characters (like "Krüger" with umlaut)
                    if 2 <= len(words) <= 4:
                        # Check if first letter of each word is uppercase (allow special chars)
                        if all(w and (w[0].isupper() or w[0] in 'ÄÖÜäöü') for w in words):
                            filtered_names.append(name)
            
            # If we filtered out all names but have titles, try to extract name from beginning of text
            # (before title keywords appear)
            if not filtered_names:
                # Look for name pattern at the very start of contact_text (before any title keywords)
                # Split text by common separators and check first part
                text_parts = re.split(r'\s+(?:Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent|T\.|E\.)', contact_text, flags=re.IGNORECASE, maxsplit=1)
                if text_parts and len(text_parts[0].strip()) > 0:
                    first_part = text_parts[0].strip()
                    # Check if first part looks like a name (2-4 capitalized words)
                    name_match = re.search(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})', first_part)
                    if name_match:
                        potential_name = name_match.group(1)
                        # Make sure it's not a title and has reasonable length
                        if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                            2 <= len(potential_name.split()) <= 4):
                            filtered_names.append(potential_name)
                
                # Also try extracting name that appears before pronoun (common pattern: "Name (pronoun)")
                # Allow special characters in names (umlauts, etc.)
                pronoun_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})\s*\(', contact_text)
                if pronoun_match:
                    potential_name = pronoun_match.group(1)
                    if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                        2 <= len(potential_name.split()) <= 4):
                        if potential_name not in filtered_names:
                            filtered_names.append(potential_name)
                
                # Also try to extract name from HTML structure - look for text nodes before the email link
                # This helps when name is in a separate element
                if link.parent:
                    # Get all text before the link in the parent (preserve order)
                    # Find all text nodes and links before this email link
                    all_siblings = []
                    for sibling in link.parent.children:
                        if sibling == link:
                            break
                        if hasattr(sibling, 'get_text'):
                            sibling_text = sibling.get_text(separator=' ', strip=True)
                            if sibling_text:
                                all_siblings.append(sibling_text)
                    
                    # Combine siblings to get full context
                    link_context = ' '.join(all_siblings)
                    if not link_context:
                        # Fallback: get all text from parent
                        link_context = link.parent.get_text(separator=' ', strip=True)
                    
                    # Split by common separators (pronoun, title keywords, phone, email)
                    name_candidates = re.split(r'\s*(?:\(he/him\)|\(she/her\)|\(they/them\)|Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|T\.|E\.|@)', link_context, flags=re.IGNORECASE, maxsplit=1)
                    if name_candidates and len(name_candidates[0].strip()) > 0:
                        first_part = name_candidates[0].strip()
                        # Check if it looks like a name (2-4 capitalized words, no title keywords)
                        # Allow umlauts and special characters
                        name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', first_part)
                        if name_match:
                            potential_name = name_match.group(1)
                            if (not any(keyword in potential_name.lower() for keyword in title_keywords) and
                                2 <= len(potential_name.split()) <= 4):
                                if potential_name not in filtered_names:
                                    filtered_names.append(potential_name)
            
            # If we still don't have names, try a more aggressive approach
            # Look for name patterns that appear at the very beginning of the contact text
            if not filtered_names:
                # Extract everything before the first title keyword, phone, or email
                text_before_metadata = re.split(r'\s*(?:Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent|Staff|T\.|E\.|\(he/him\)|\(she/her\)|@)', contact_text, flags=re.IGNORECASE, maxsplit=1)[0]
                if text_before_metadata:
                    # Look for name pattern in this text
                    name_match = re.search(r'^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})', text_before_metadata.strip())
                    if name_match:
                        potential_name = name_match.group(1)
                        # Make sure it's not a title
                        if not any(keyword in potential_name.lower() for keyword in title_keywords):
                            filtered_names.append(potential_name)
            
            names = filtered_names if filtered_names else unique_names  # Fallback to original if filtering removed everything
            
            # If we have multiple name candidates, prefer the one that appears first in the text
            # and is not a title
            if len(names) > 1:
                # Find the position of each name in the contact_text
                name_positions = []
                for name in names:
                    pos = contact_text.find(name)
                    if pos >= 0:
                        name_positions.append((pos, name))
                # Sort by position and take the first one
                if name_positions:
                    name_positions.sort(key=lambda x: x[0])
                    names = [name_positions[0][1]]
            
            # Extract phone (pattern: T. (040) 8998-XXXX or +49 (0)40 8998-XXXX)
            # More flexible pattern to handle various phone formats
            phone_pattern = r'(?:T\.|Phone:?|Tel\.?)\s*[+\d\s\-\(\)]{8,}|\(\d{3,4}\)\s*\d{4,}[\s\-]?\d+|[\+\d\s\-\(\)]{10,}'
            phones = re.findall(phone_pattern, contact_text)
            # Clean up phone numbers (remove extra spaces, normalize)
            phones = [re.sub(r'\s+', ' ', p.strip()) for p in phones if len(p.strip()) >= 8]
            
            # Extract title (pattern: Head of..., Manager..., etc.)
            # More comprehensive pattern to catch various title formats
            # Stop at phone numbers (T. or E.) to avoid capturing them
            # Pattern 1: Full title with "of/for" (e.g., "Head of Recruitment and Employer Branding")
            # Stop before phone (T.) or email (E.) markers
            title_pattern1 = r'\b(Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent)\s+(?:of|for)?\s+[^\.\(\)T]+?(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            # Pattern 2: Title without "of/for" (e.g., "HR Manager Recruitment Technical & Scientific Staff")
            # Stop before phone/email markers
            title_pattern2 = r'\b(Head|Manager|Leader|Trainer|Assistant|Team|HR|Recruitment|Employer|Branding|Scientist|Technician|Clerk|Assistent)\s+[A-Z][^\.\(\)T]+?(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            # Pattern 3: Multi-word titles (e.g., "Team Leader Recruitment", "HR Team Assistent Recruitment")
            title_pattern3 = r'\b(Team\s+(?:Leader|Assistent|Manager)\s+[^\.\(\)T]+?|HR\s+(?:Manager|Team|Assistent)\s+[^\.\(\)T]+?|Head\s+of\s+[^\.\(\)T]+?)(?=\s+(?:T\.|E\.|\(he/him\)|\(she/her\))|$)'
            
            titles = []
            # Try all patterns
            titles.extend(re.findall(title_pattern1, contact_text, re.IGNORECASE))
            titles.extend(re.findall(title_pattern2, contact_text, re.IGNORECASE))
            titles.extend(re.findall(title_pattern3, contact_text, re.IGNORECASE))
            
            # Remove duplicates and clean up titles
            seen_titles = set()
            unique_titles = []
            for title in titles:
                title_clean = re.sub(r'\s+', ' ', title.strip())
                # Remove trailing punctuation and normalize
                title_clean = re.sub(r'[\.\,]+$', '', title_clean).strip()
                # Remove phone number patterns that might be captured (e.g., "T" at the end)
                title_clean = re.sub(r'\s+T\.?\s*$', '', title_clean, flags=re.IGNORECASE)
                title_clean = re.sub(r'\s+E\.?\s*$', '', title_clean, flags=re.IGNORECASE)
                # Remove phone numbers that might be in the title
                title_clean = re.sub(r'\s*T\.\s*\(?\d+\)?.*$', '', title_clean, flags=re.IGNORECASE)
                title_clean = re.sub(r'\s*E\.\s*[a-zA-Z0-9._%+-]+@.*$', '', title_clean, flags=re.IGNORECASE)
                # Remove email addresses
                title_clean = re.sub(r'\s+[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}.*$', '', title_clean)
                title_clean = title_clean.strip()
                if title_clean and title_clean.lower() not in seen_titles and len(title_clean) > 5:
                    seen_titles.add(title_clean.lower())
                    unique_titles.append(title_clean)
            
            titles = unique_titles
            
            # Extract pronoun (pattern: (he/him), (she/her))
            pronoun_pattern = r'\((he|she|they)/(him|her|them)\)'
            pronouns = re.findall(pronoun_pattern, contact_text)
            
            # Only add if we have at least email (required) and either name or phone (at least one other field)
            # BUT: Don't use titles as names - if we only have titles and no proper names, set name to None
            final_name = None
            if names:
                # Make absolutely sure the name is not a title
                name_candidate = names[0]
                name_lower = name_candidate.lower()
                # Double-check: if it contains title keywords, it's not a name
                if not any(keyword in name_lower for keyword in title_keywords):
                    final_name = name_candidate
                else:
                    # Name candidate is actually a title, don't use it
                    print(f"[DEBUG] Rejected name candidate '{name_candidate}' - it's a title, not a name")
            
            if email and (final_name or phones or titles):
                contact_blocks.append({
                    'email': email,
                    'name': final_name,
                    'phone': phones[0] if phones else None,
                    'title': titles[0] if titles else None,
                    'pronoun': pronouns[0] if pronouns else None,
                    'full_text': contact_text
                })
                print(f"[DEBUG] Extracted contact block: email={email}, name={final_name if final_name else 'None'}, phone={phones[0] if phones else 'None'}, title={titles[0] if titles else 'None'}")
            else:
                print(f"[DEBUG] Skipped contact block: email={email}, has_name={bool(final_name)}, has_phone={bool(phones)}, has_title={bool(titles)}")
    
    return contact_blocks


def reconstruct_contact_structure(contact_blocks, page_title=None):
    """
    Reconstruct markdown structure from extracted contact blocks.
    
    Creates:
    - Page title from URL or content
    - Section headings
    - List structure for contact entries
    """
    markdown = []
    
    # Add page title
    if page_title:
        markdown.append(f"# {page_title}")
    else:
        markdown.append("# Contact Information")
    markdown.append("")
    
    # Add section heading
    markdown.append("## Contact Details")
    markdown.append("")
    
    # Add contact entries as structured list
    for contact in contact_blocks:
        entry = []
        
        # Name with pronoun if available
        # Always include name if available, even if it's None (will use email as fallback)
        if contact['name']:
            name_line = f"- **{contact['name']}**"
            if contact['pronoun']:
                name_line += f" ({contact['pronoun'][0]}/{contact['pronoun'][1]})"
            entry.append(name_line)
        elif contact['email']:
            # If no name, use email address as identifier
            email_local = contact['email'].split('@')[0].replace('.', ' ').title()
            name_line = f"- **{email_local}**"
            if contact['pronoun']:
                name_line += f" ({contact['pronoun'][0]}/{contact['pronoun'][1]})"
            entry.append(name_line)
        
        # Title
        if contact['title']:
            # Clean title: remove any trailing phone/email markers
            title = contact['title'].strip()
            # Remove trailing "T", "E", or phone/email patterns
            title = re.sub(r'\s+[TE]\.?\s*$', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s+T\.\s*\(.*$', '', title)
            title = re.sub(r'\s+E\.\s*[a-zA-Z0-9._%+-]+@.*$', '', title)
            entry.append(f"  - Title: {title}")
        
        # Phone
        if contact['phone']:
            # Fix phone format: ensure space after colon and normalize
            phone = contact['phone'].strip()
            # Normalize phone format: "T. (040) 8998-4219" or "T.(040) 8998-4219" -> "T. (040) 8998-4219"
            phone = re.sub(r'T\.\s*\(', 'T. (', phone)
            phone = re.sub(r'T\.\(', 'T. (', phone)
            # Remove "T." prefix if it's duplicated (e.g., "T. T. (040)")
            phone = re.sub(r'^T\.\s+T\.\s+', 'T. ', phone)
            entry.append(f"  - Phone: {phone}")
        
        # Email (always include if available)
        if contact['email']:
            entry.append(f"  - Email: [{contact['email']}](mailto:{contact['email']})")
        
        if entry:
            markdown.extend(entry)
            markdown.append("")
    
    return '\n'.join(markdown)


# ============================================================================
# Markdown Post-Processing
# ============================================================================

def clean_markdown_links_post_process(markdown_text):
    """
    Post-process markdown to clean link syntax by removing whitespace.
    
    This should be run after HTML→Markdown conversion.
    """
    if not markdown_text:
        return markdown_text
    
    # Remove spaces in link brackets
    markdown_text = re.sub(r'\[\s+', '[', markdown_text)
    markdown_text = re.sub(r'\s+\]', ']', markdown_text)
    
    # Remove spaces in link parentheses
    markdown_text = re.sub(r'\(\s+', '(', markdown_text)
    markdown_text = re.sub(r'\s+\)', ')', markdown_text)
    
    # Remove spaces after colons in URLs/mailto
    markdown_text = re.sub(r':\s+', ':', markdown_text)
    
    # Remove spaces before/after email addresses in links (more specific)
    # Pattern: [ space email space ] -> [email]
    markdown_text = re.sub(r'\[\s+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\s+\]', r'[\1]', markdown_text)
    
    return markdown_text


# ============================================================================
# Invisible Link Detection and HTML Filtering
# ============================================================================

def get_invisible_link_urls(html_content, excluded_selector_list):
    """
    Extract URLs of links that match disabled/inactive/hidden selectors.
    These are links that should NOT be crawled because they're not visible to users.
    
    Args:
        html_content: HTML string of the page
        excluded_selector_list: List of CSS selectors matching invisible elements
        
    Returns:
        Set of URLs that are invisible/inactive and should be skipped
    """
    if not html_content or not BEAUTIFULSOUP_AVAILABLE:
        return set()
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        invisible_urls = set()
        
        # For each CSS selector, find matching links and extract their href
        for selector in excluded_selector_list:
            try:
                # Try to use CSS selector
                matching_elements = soup.select(selector)
                for elem in matching_elements:
                    # Get all links in this element
                    if elem.name == 'a':
                        href = elem.get('href', '')
                        if href:
                            invisible_urls.add(href)
                    else:
                        # Element is not a link, find links inside it
                        for link in elem.find_all('a', href=True):
                            href = link.get('href', '')
                            if href:
                                invisible_urls.add(href)
            except Exception:
                # Skip malformed selectors
                continue
        
        return invisible_urls
    except Exception as e:
        print(f"[WARNING] Error detecting invisible links: {e}")
        return set()


def filter_inactive_elements_from_html(html_content, selectors_to_remove):
    """
    GROUP 6 Option B fallback: Remove elements matching CSS selectors from HTML.
    This function is used if PruningContentFilter doesn't support excluded_selectors.
    
    Args:
        html_content: Raw HTML string from crawl
        selectors_to_remove: List of CSS selectors to filter out
        
    Returns:
        Filtered HTML string with matching elements removed
    """
    if not html_content or not selectors_to_remove or not BEAUTIFULSOUP_AVAILABLE:
        return html_content
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Remove elements matching each selector
        removed_count = 0
        for selector in selectors_to_remove:
            try:
                for element in soup.select(selector):
                    element.decompose()  # Remove element and its contents
                    removed_count += 1
            except Exception:
                # Invalid selector or parsing error - skip this selector
                pass
        
        if removed_count > 0:
            print(f"[GROUP 6 FALLBACK] Removed {removed_count} elements matching {len(selectors_to_remove)} CSS selectors from HTML")
        
        return str(soup)
    except Exception as e:
        # Filtering failed - return original HTML
        print(f"[GROUP 6 FALLBACK] Warning: Could not filter inactive elements: {str(e)[:100]}")
        return html_content


# ============================================================================
# Content Deduplication
# ============================================================================

def get_content_hash(content_str):
    """
    Generate a hash of page content for deduplication.
    Uses Crawl4AI's ContentHash if available, else fallback to SHA256.
    
    Args:
        content_str: Markdown or HTML content to hash
        
    Returns:
        Hash string representing the content
    """
    if not content_str:
        return None
    
    if CONTENT_HASH_AVAILABLE:
        try:
            # Use Crawl4AI's ContentHash for deduplication
            hasher = ContentHash()
            return hasher.hash(content_str)
        except Exception:
            # Fallback if ContentHash fails
            pass
    
    # Fallback: Use SHA256 hash of content
    return hashlib.sha256(content_str.encode('utf-8')).hexdigest()


def is_duplicate_content(content_str, seen_content_hashes, dedup_enabled, url=None):
    """
    Check if content is duplicate (seen before).
    
    Args:
        content_str: Markdown or HTML content to check
        seen_content_hashes: Dict mapping content_hash -> original_url (mutated in-place)
        dedup_enabled: Whether deduplication is enabled
        url: Current URL (for logging)
        
    Returns:
        Tuple (is_duplicate: bool, original_url: str or None, hash: str)
    """
    if not content_str or not dedup_enabled:
        return False, None, None
    
    try:
        content_hash = get_content_hash(content_str)
        if not content_hash:
            return False, None, None
        
        if content_hash in seen_content_hashes:
            # Duplicate found
            original_url = seen_content_hashes[content_hash]
            return True, original_url, content_hash
        else:
            # New content - track it
            seen_content_hashes[content_hash] = url or "unknown"
            return False, None, content_hash
    except Exception:
        # Deduplication failed - continue without it
        return False, None, None


def extract_tables_and_images(result, result_is_pdf, pdf_support_available=False):
    """Extract tables and image references from a crawl result.

    For HTML pages: Uses Indico event extraction or DOM-order table extraction.
    Falls back to Crawl4AI raw tables if extraction fails.
    For PDFs: Uses Crawl4AI tables and extracts image references.

    Args:
        result: Crawl result object with .html, .url, .tables, .media attributes.
        result_is_pdf: Whether the result is from a PDF page.
        pdf_support_available: Whether PDF support is available.

    Returns:
        (tables_markdown, image_refs_markdown): tuple of strings.
    """
    tables_markdown = ""
    image_refs_markdown = ""

    # HTML table extraction (Indico + DOM-order + Crawl4AI fallback)
    if not result_is_pdf and hasattr(result, 'html') and result.html and BEAUTIFULSOUP_AVAILABLE:
        try:
            # Indico event pages have a specific structure requiring custom extraction
            current_url = result.url if hasattr(result, 'url') else None
            if current_url and is_indico_url(current_url):
                print(f"[INFO] Detected Indico event page - using specialized extractor")
                indico_content = extract_indico_event(result.html, url=current_url)
                if indico_content:
                    tables_markdown = indico_content
                    print(f"[INFO] Indico extraction: {len(indico_content)} chars extracted")
                    dom_ordered_content = []
                else:
                    print(f"[WARNING] Indico extraction returned empty, using general extraction")
                    dom_ordered_content = _table_processing.extract_headings_and_tables_in_dom_order(result.html, url=result.url)
            else:
                dom_ordered_content = _table_processing.extract_headings_and_tables_in_dom_order(result.html, url=result.url)

            # Only process DOM extraction if we didn't use Indico extractor
            if dom_ordered_content:
                print(f"[DEBUG] DOM-order extraction: Found {len(dom_ordered_content)} content items")
                tables_markdown = _table_processing.format_tables_with_headings_as_markdown(dom_ordered_content)
                if tables_markdown:
                    table_count = sum(1 for item in dom_ordered_content if item['type'] == 'table')
                    heading_count = sum(1 for item in dom_ordered_content if item['type'] == 'heading')
                    print(f"[INFO] DOM-order extraction: Formatted {table_count} table(s) and {heading_count} heading(s)")
                else:
                    print(f"[DEBUG] DOM-order extraction: No tables formatted (empty result)")
        except Exception as e:
            print(f"[WARNING] Hybrid table extraction failed: {e}")
            import traceback
            traceback.print_exc()
            # Fallback: Use Crawl4AI tables if Hybrid extraction fails
            if hasattr(result, 'tables') and result.tables:
                print(f"[DEBUG] Falling back to Crawl4AI table extraction")
                for idx, crawl_table in enumerate(result.tables, 1):
                    if isinstance(crawl_table, dict):
                        headers = crawl_table.get('headers', [])
                        rows = crawl_table.get('rows', []) or crawl_table.get('data', [])
                        if rows:
                            if headers:
                                tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                                tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                            for row in rows:
                                tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                            tables_markdown += "\n"

    # Fallback for PDFs: Use Crawl4AI tables
    if result_is_pdf and hasattr(result, 'tables') and result.tables:
        print(f"[DEBUG] PDF: Using Crawl4AI table extraction ({len(result.tables)} table(s))")
        for idx, crawl_table in enumerate(result.tables, 1):
            if isinstance(crawl_table, dict):
                headers = crawl_table.get('headers', [])
                rows = crawl_table.get('rows', []) or crawl_table.get('data', [])
                if rows:
                    if headers:
                        tables_markdown += "| " + " | ".join(str(h) for h in headers) + " |\n"
                        tables_markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                    for row in rows:
                        tables_markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                    tables_markdown += "\n"

    # Extract image references (for PDFs)
    if result_is_pdf and pdf_support_available:
        if hasattr(result, 'media') and result.media:
            images = result.media.get("images", [])
            if images:
                print(f"[PDF] Extracted {len(images)} image(s) from {result.url}")
                image_refs_markdown = "\n\n## Extracted Images\n\n"
                for idx, img_info in enumerate(images, 1):
                    img_path = img_info.get('path', '')
                    if img_path:
                        img_filename = Path(img_path).name
                        image_refs_markdown += f"![Image {idx}](extracted_images/{img_filename})\n\n"

    return tables_markdown, image_refs_markdown


# Binary extensions checked in the post-crawl filter (complement to the pre-crawl
# check in url_utils._is_valid_crawl_url).  BFS bypasses _is_valid_crawl_url, so
# image/binary/source URLs that slip through are blocked here before saving.
_BINARY_EXTENSIONS_POST_CRAWL = (
    '.gif', '.jpg', '.jpeg', '.png', '.webp', '.svg', '.bmp', '.ico', '.tiff',
    '.zip', '.tar', '.gz', '.rar', '.doc', '.docx', '.xls', '.xlsx',
    '.ppt', '.pptx', '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.flv',
    '.tex', '.eps', '.ps', '.tgz', '.bib', '.f90', '.f77', '.f',
    '.bbl', '.blg', '.cls', '.sty', '.dtx', '.ins', '.aux',
    '.atom',
)

# Login-wall keywords that indicate a page is serving a login form instead of
# real content.  Combined with a short-page threshold (<300 chars).
_LOGIN_CONTENT_KEYWORDS = (
    'please log in',
    'sign in to',
    'login required',
    'you must be logged in',
    'access denied',
    'enter your username',
    'forgot your password',
)


def filter_result_pre_save(result, scope_filter_fn=None, login_filter_fn=None,
                           varnish_503_markers=None):
    """Apply the 6-stage content-filter pipeline to a crawl result.

    Checks (in order):
      1. Invalid/empty result
      2. Scope gate (via scope_filter_fn callback)
      3. Login/auth URL (via login_filter_fn callback)
      4. Binary file extension
      5. Content-based login-wall (short markdown + login keywords)
      6. Minimum content (<50 chars body)
      7. Varnish 503 marker detection

    Returns (action, reason) where action is one of:
      - 'skip'        → result should be silently skipped
      - 'varnish_503' → result should be queued for 503 retry
      - 'accept'      → result passed all filters

    reason is a short human-readable string (empty for 'accept').
    """
    from urllib.parse import urlparse

    # --- 1. Invalid / empty result ---
    if not result or not getattr(result, 'url', None):
        return ('skip', 'invalid_result')
    url = result.url
    if _table_processing._is_empty_or_whitespace(str(url)):
        return ('skip', 'empty_url')

    # --- 2. Scope gate ---
    if scope_filter_fn is not None:
        if not scope_filter_fn(url):
            redirected = getattr(result, 'redirected_url', None)
            if not redirected or not scope_filter_fn(redirected):
                return ('skip', 'out_of_scope')

    # --- 3. Login / auth URL filter ---
    if login_filter_fn is not None:
        redirected = getattr(result, 'redirected_url', None)
        if login_filter_fn(url) or (redirected and login_filter_fn(redirected)):
            return ('skip', 'login_auth_url')

    # --- 4. Binary extension filter ---
    url_path_lower = urlparse(url).path.lower()
    if any(url_path_lower.endswith(ext) for ext in _BINARY_EXTENSIONS_POST_CRAWL):
        return ('skip', 'binary_extension')

    # --- 4b. Printversion / @@siteview filter (Root Cause 2, part 4) ---
    # Plone CMS exposes ?printversion=1 and /@@siteview URLs that render
    # navigation-only copies of every page — zero unique body content.
    # These should have been blocked by BFS filter_chain, but that is
    # silently broken when FilterChain import fails (Section 4 known bug).
    redirected = getattr(result, 'redirected_url', None)
    _check_url = redirected if redirected else url
    if re.search(r'[?&]printversion=', _check_url) or '/@@siteview' in _check_url:
        return ('skip', 'printversion_siteview')

    # --- 5. Content-based login-wall ---
    _md_raw = getattr(result, 'markdown', None)
    if _md_raw is not None:
        if hasattr(_md_raw, 'raw_markdown'):
            _md_text = _md_raw.raw_markdown or ''
        elif isinstance(_md_raw, str):
            _md_text = _md_raw
        else:
            _md_text = ''
    else:
        _md_text = ''
    md_stripped = _md_text.strip()

    if len(md_stripped) < 300 and any(kw in md_stripped.lower() for kw in _LOGIN_CONTENT_KEYWORDS):
        return ('skip', 'login_wall_content')

    # --- 6. Minimum content guard ---
    body_text = re.sub(r'^#\s*Source\s*URL\s*\n+\S+\s*\n*', '', md_stripped, flags=re.IGNORECASE).strip()
    if len(body_text) < 50:
        return ('skip', 'blank_page')

    # --- 7. Varnish 503 detection ---
    if varnish_503_markers:
        if any(marker in body_text for marker in varnish_503_markers):
            return ('varnish_503', 'varnish_503')

    return ('accept', '')


async def retry_varnish_503_pages(
    crawler,
    retry_queue,
    retry_config,
    varnish_503_markers,
    crawled_urls_with_depth_merged,
    additional_urls_with_depth_merged,
    max_depth,
    output_dir,
    seen_final_urls,
    all_successful_urls,
    all_urls_by_depth,
    login_filter_fn=None,
    ui_only_query_params=None,
    content_critical_params=None,
    scope_filter_fn=None,
    exclusion_re=None,
):
    """Re-crawl URLs that returned Varnish 503 backend errors during the main pass.

    Waits 10 seconds for Plone backends to recover, then re-crawls each URL
    one at a time using the provided retry_config (which must have cache_mode='bypass').

    After a successful retry, child ``<a href>`` links are extracted from the
    page HTML so their parent pages' children are not lost due to the original
    503 failure during BFS.  Discovered child URLs (with ``parent_depth + 1``)
    are returned so the caller can crawl them in a follow-up pass.

    Mutates: seen_final_urls, all_successful_urls, all_urls_by_depth.
    Returns: (retry_saved, retry_still_503, pages_processed_delta, discovered_child_urls)
             where discovered_child_urls is a dict {normalized_url: depth}.
    """
    import asyncio

    print(f"\n[503-RETRY] {len(retry_queue)} URLs returned Varnish 503 during crawl.")
    print(f"[503-RETRY] Waiting 10 seconds for Plone backends to recover...")
    await asyncio.sleep(10)

    _retry_saved = 0
    _retry_still_503 = 0
    _discovered_child_urls = {}  # {normalized_url: depth} — child links from retried pages
    # FIX (Problem 2): Skip calendar view URLs — they have no Varnish cache and
    # put direct load on the server.  Same patterns as PATH A exclusion_patterns.
    _CALENDAR_VIEW_RE = re.compile(
        r'[?&]view=(day|week|month|workWeek)([&]|$)|[?&]notoolbar=\d',
        re.IGNORECASE,
    )

    for _retry_url in retry_queue:
        if _CALENDAR_VIEW_RE.search(_retry_url):
            print(f'[503-RETRY] Skipping calendar view URL: {_retry_url}')
            continue
        print(f'[503-RETRY] Retrying: {_retry_url}')
        try:
            _r = await crawler.arun(_retry_url, config=retry_config)
            if not _r or not getattr(_r, 'url', None):
                continue

            # Extract markdown text
            _r_md = getattr(_r, 'markdown', None)
            if _r_md is not None:
                if hasattr(_r_md, 'raw_markdown'):
                    _r_text = (_r_md.raw_markdown or '').strip()
                elif isinstance(_r_md, str):
                    _r_text = _r_md.strip()
                else:
                    _r_text = ''
            else:
                _r_text = ''

            # Strip Source URL header, then check for still-503 / blank
            _r_body = re.sub(r'^#\s*Source\s*URL\s*\n+\S+\s*\n*', '', _r_text, flags=re.IGNORECASE).strip()
            if len(_r_body) < 50 or any(m in _r_body for m in varnish_503_markers):
                print(f'[503-RETRY] Still 503/blank after retry, discarding: {_retry_url}')
                _retry_still_503 += 1
                continue

            # FIX (Issue 9): Apply GROUP 1 login filter to the retry result.
            # When Plone recovers from overload it may redirect auth-required pages
            # to login_form — the retry code must not save those as content.
            _r_final_for_login_check = getattr(_r, 'redirected_url', None) or getattr(_r, 'url', _retry_url)
            if login_filter_fn and login_filter_fn(_r_final_for_login_check):
                print(f'[503-RETRY] Skipping login redirect: {_retry_url} → {_r_final_for_login_check}')
                continue

            # Determine depth from existing maps (URL was already BFS-discovered)
            _r_final_url = getattr(_r, 'redirected_url', None) or _r.url
            _r_norm_final = _url_utils._normalize_url(_r_final_url)
            _r_norm_orig = _url_utils._normalize_url(_retry_url)
            # FIX (Section 6): Replace or-chain with explicit is-not-None guards
            # so that depth 0 (seed URLs) is not treated as falsy.
            _r_depth = 2  # Conservative default for BFS-discovered retry pages
            if crawled_urls_with_depth_merged:
                _d = crawled_urls_with_depth_merged.get(_r_norm_final)
                if _d is None:
                    _d = crawled_urls_with_depth_merged.get(_r_norm_orig)
                if _d is not None:
                    _r_depth = _d
            if additional_urls_with_depth_merged:
                _d = additional_urls_with_depth_merged.get(_r_norm_final)
                if _d is None:
                    _d = additional_urls_with_depth_merged.get(_r_norm_orig)
                if _d is not None:
                    _r_depth = min(_r_depth, _d)
            _r_depth = min(int(_r_depth), max_depth)

            # FIX (Root Cause 2, part 2): Use dedup key (UI params stripped)
            # for filename so printversion/siteview URLs get clean filenames.
            _r_dedup_key = _url_utils.normalize_url_for_dedup(_r_norm_final, ui_only_query_params or set(), content_critical_params or set()) if _r_norm_final else _r_norm_final
            _r_url_for_file = _r_dedup_key if _r_dedup_key else (_r_final_url if _r_final_url else _retry_url)
            _r_url_safe = (_r_url_for_file
                           .replace("https://", "").replace("http://", "")
                           .replace("/", "_").replace(":", "_"))
            if len(_r_url_safe) > 200:
                _r_url_safe = _r_url_safe[:200]
            _r_depth_dir = output_dir / f"depth_{_r_depth}"
            _r_depth_dir.mkdir(exist_ok=True)
            _r_filename = _r_depth_dir / f"{_r_url_safe}.md"

            # Dedup check: skip if this dedup key was already saved by an
            # earlier retry entry (e.g. multiple calendar ?date= variants all
            # map to the same stripped key and would overwrite the same file).
            if _r_dedup_key and _r_dedup_key in seen_final_urls:
                print(f'[503-RETRY] Skipping dedup (already saved): {_retry_url}')
                continue

            # Write file and update tracking state
            _r_filename.write_text(_r_text, encoding="utf-8")
            # FIX (Root Cause 2, part 3): Use normalize_url_for_dedup so the
            # key format matches STEP 8's seen_final_urls entries.
            _r_dedup_seen = _url_utils.normalize_url_for_dedup(_r_norm_final, ui_only_query_params or set(), content_critical_params or set()) if _r_norm_final else (_url_utils.normalize_url_for_dedup(_r_norm_orig, ui_only_query_params or set(), content_critical_params or set()) if _r_norm_orig else _r_norm_orig)
            seen_final_urls.add(_r_dedup_seen if _r_dedup_seen else _r_norm_orig)
            all_successful_urls.append(_retry_url)
            _depth_key = str(_r_depth)
            if _depth_key not in all_urls_by_depth:
                all_urls_by_depth[_depth_key] = []
            all_urls_by_depth[_depth_key].append({
                'original_url': _retry_url,
                'final_url': _r_final_url,
                'is_redirect': (_retry_url != _r_final_url)
            })
            _retry_saved += 1
            print(f'[503-RETRY] Saved: {_retry_url} → {_r_filename}')

            # --- Extract child <a href> links from the retried HTML ----------
            # The original 503 during BFS meant this page's children were never
            # discovered.  Now that the page loaded successfully, harvest its
            # outgoing links so the caller can crawl them in a follow-up pass.
            _child_depth = _r_depth + 1
            if _child_depth <= max_depth:
                _r_html = getattr(_r, 'html', None) or ''
                if _r_html:
                    from html.parser import HTMLParser as _HP

                    class _RetryLinkExtractor(_HP):
                        def __init__(self):
                            super().__init__()
                            self.links = []
                        def handle_starttag(self, tag, attrs):
                            if tag == 'a':
                                for attr, val in attrs:
                                    if attr == 'href' and val:
                                        self.links.append(val)

                    _lp = _RetryLinkExtractor()
                    try:
                        _lp.feed(_r_html)
                    except Exception:
                        _lp.links = []
                    _child_base = _r_final_url or _retry_url
                    for _raw_href in _lp.links:
                        if (not _raw_href
                                or _raw_href.startswith('#')
                                or _raw_href.startswith('javascript:')
                                or _raw_href.startswith('mailto:')):
                            continue
                        _abs_child = urljoin(_child_base, _raw_href)
                        # Apply scope and exclusion filters
                        if scope_filter_fn and not scope_filter_fn(_abs_child):
                            continue
                        if login_filter_fn and login_filter_fn(_abs_child):
                            continue
                        if exclusion_re and exclusion_re.search(_abs_child):
                            continue
                        _child_norm = _url_utils._normalize_url(_abs_child)
                        if not _child_norm:
                            continue
                        # Skip URLs already crawled or already seen
                        _child_dedup = _url_utils.normalize_url_for_dedup(
                            _child_norm,
                            ui_only_query_params or set(),
                            content_critical_params or set(),
                        ) if _child_norm else _child_norm
                        if _child_dedup and _child_dedup in seen_final_urls:
                            continue
                        # Keep shallowest depth if already discovered
                        _existing_d = _discovered_child_urls.get(_child_norm)
                        if _existing_d is None or _child_depth < _existing_d:
                            _discovered_child_urls[_child_norm] = _child_depth
                    if _discovered_child_urls:
                        print(f'[503-RETRY] Extracted {len(_discovered_child_urls)} child link(s) from retried pages so far')

        except Exception as _retry_err:
            print(f'[503-RETRY] Error retrying {_retry_url}: {_retry_err}')

    print(f'[503-RETRY] Complete: {_retry_saved} saved, {_retry_still_503} still 503/blank (discarded).')
    if _discovered_child_urls:
        print(f'[503-RETRY] Discovered {len(_discovered_child_urls)} child URL(s) from successfully retried pages.')
    return _retry_saved, _retry_still_503, _retry_saved, _discovered_child_urls


# ---------------------------------------------------------------------------
# assign_page_depth  (extracted Step 7j)
# ---------------------------------------------------------------------------

def assign_page_depth(
    normalized_original,
    normalized_final,
    seed_urls_normalized,
    crawled_urls_with_depth_merged,
    additional_urls_with_depth_merged,
    result,
    max_depth,
):
    """Determine the depth (0 … *max_depth*) for a crawled page.

    Resolution order
    ----------------
    1. **Seed check** – if either normalised URL is in *seed_urls_normalized*,
       return 0 immediately.
    2. **Additional-URL map** (priority) – if the URL is in
       *additional_urls_with_depth_merged* (e.g. child URLs discovered from
       retried 503 pages), use that depth directly.  This prevents the BFS
       fallback depth (often 1) from overriding the correct parent+1 depth.
    3. **BFS map lookup** – check *crawled_urls_with_depth_merged* for both
       the original and the final URL, pick the minimum non-zero candidate.
    4. **Metadata fallback** – ``result.metadata['depth']`` or
       ``result.depth``.
    5. **Default fallback** – non-seed pages with no depth info default to 1.
    6. **Capping** – the final value is clamped to *max_depth*.
    """
    # 1) Seed URLs always get depth 0
    if normalized_original in seed_urls_normalized or normalized_final in seed_urls_normalized:
        return 0

    # 2) Additional-URL map takes priority (e.g. retry-discovered child URLs)
    if additional_urls_with_depth_merged is not None:
        _additional_candidates = []
        if normalized_final in additional_urls_with_depth_merged:
            _additional_candidates.append(additional_urls_with_depth_merged[normalized_final])
        if normalized_original in additional_urls_with_depth_merged:
            _additional_candidates.append(additional_urls_with_depth_merged[normalized_original])
        _additional_nz = [d for d in _additional_candidates if d > 0]
        if _additional_nz:
            return min(min(_additional_nz), max_depth)

    # 3) BFS map lookup
    depth_candidates = []
    if crawled_urls_with_depth_merged is not None:
        if normalized_final in crawled_urls_with_depth_merged:
            depth_candidates.append(crawled_urls_with_depth_merged[normalized_final])
        if normalized_original in crawled_urls_with_depth_merged:
            depth_candidates.append(crawled_urls_with_depth_merged[normalized_original])

    non_zero = [d for d in depth_candidates if d > 0]
    if non_zero:
        depth = min(non_zero)
    else:
        # 3) Fallback: Result metadata (from BFSDeepCrawlStrategy)
        depth = 0
        if hasattr(result, 'metadata') and result.metadata:
            depth = result.metadata.get('depth', 0)
        elif hasattr(result, 'depth'):
            depth = getattr(result, 'depth', 0) or 0
        # 4) Fallback: non-seed with no map/metadata -> depth 1
        if depth == 0:
            depth = 1

    # 5) Cap depth at max_depth
    if depth > max_depth:
        depth = max_depth

    return depth


# ---------------------------------------------------------------------------
# count_internal_links  (extracted Step 7k)
# ---------------------------------------------------------------------------

def count_internal_links(result, base_url):
    """Count links in *result*.html that point to the same domain as *base_url*.

    Returns 0 when BeautifulSoup is unavailable, the result has no HTML, or
    on any parsing error.
    """
    if not BEAUTIFULSOUP_AVAILABLE:
        return 0
    if not hasattr(result, 'html') or not result.html:
        return 0

    try:
        soup = _get_cached_soup(result)
        if not soup:
            return 0

        base_domain = _url_utils._normalize_domain(
            urlparse(base_url).netloc
        ) if base_url else ''

        count = 0
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if (not href
                    or href.startswith('#')
                    or href.startswith('javascript:')
                    or href.startswith('mailto:')):
                continue
            absolute_url = urljoin(base_url, href) if base_url else href
            link_domain = _url_utils._normalize_domain(
                urlparse(absolute_url).netloc
            )
            if link_domain == base_domain:
                count += 1
        return count
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# extract_result_urls  (extracted Step 7l)
# ---------------------------------------------------------------------------

def extract_result_urls(result):
    """Extract original, final (after redirect), and normalised URLs from *result*.

    Returns ``(original_url, final_url, is_redirect,
               normalized_original, normalized_final)``.
    """
    original_url = result.url if hasattr(result, 'url') and result.url else None
    final_url = None
    is_redirect = False

    if hasattr(result, 'redirected_url') and result.redirected_url:
        final_url = result.redirected_url
        is_redirect = (original_url != final_url)
    else:
        final_url = original_url

    normalized_original = _url_utils._normalize_url(original_url)
    normalized_final = _url_utils._normalize_url(final_url)

    return original_url, final_url, is_redirect, normalized_original, normalized_final


# ---------------------------------------------------------------------------
# is_404_without_content  (extracted Step 7l)
# ---------------------------------------------------------------------------

def is_404_without_content(result, normalized_final):
    """Return ``True`` if *result* is a 404 page with no meaningful content.

    A page is considered to have content when its HTML is longer than 100
    characters or its best-available markdown exceeds 100 characters.

    404 is detected either by ``/404/`` in the normalised final URL or by
    ``result.status_code == 404``.
    """
    has_content = False

    # Check HTML length
    if hasattr(result, 'html') and result.html and len(result.html.strip()) > 100:
        has_content = True
    elif hasattr(result, 'markdown'):
        md = None
        if hasattr(result.markdown, 'fit_markdown') and result.markdown.fit_markdown:
            md = result.markdown.fit_markdown
        elif hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
            md = result.markdown.raw_markdown
        elif isinstance(result.markdown, str):
            md = result.markdown
        if md and len(md.strip()) > 100:
            has_content = True

    if has_content:
        return False

    # No meaningful content — check 404 signals
    if normalized_final and '/404/' in normalized_final:
        return True
    if hasattr(result, 'status_code') and result.status_code == 404:
        return True

    return False
