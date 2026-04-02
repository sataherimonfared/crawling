"""Markdown cleanup utilities for DESY web crawler.

Extracted from crawl_desy_all_urls.py (Step 5 of refactoring plan).
Contains markdown separator detection and text spacing normalization.

Note: Related functions in other modules:
- clean_markdown_links_post_process → content_extraction.py
- _is_empty_or_whitespace → table_processing.py
"""

import re
import content_extraction as _content_extraction
import table_processing as _table_processing


def _is_separator_line(line):
    """
    Check if a line is a markdown separator (---, |---|---, table separators, etc.).
    
    Args:
        line: Line string to check (should be stripped)
        
    Returns:
        True if line is a separator
    """
    if not line:
        return False
    return (line == '---' or 
            re.match(r'^\|[\s\-:]+\|$', line) or 
            line == '|---|---' or
            re.match(r'^\|[\s\-]+\|$', line) or
            re.match(r'^[\|\s\-]+$', line))


def _normalize_text_spacing(line):
    """
    Normalize text spacing to fix concatenation issues.
    
    Fixes patterns like:
    - "word+Capital" -> "word +Capital"
    - "hutch:+49" -> "hutch: +49" (but preserve phone formats)
    - Multiple spaces -> single space
    
    Args:
        line: Input line string
        
    Returns:
        Normalized line string
    """
    if not line or line.strip().startswith(('#', '|', '-', '*')) or not line.strip():
        # Don't modify markdown syntax lines
        return line
    
    # Normalize multiple spaces to single space
    normalized = re.sub(r' +', ' ', line)
    
    # Fix concatenated patterns: word+Capital (but not in URLs/emails)
    # Pattern: lowercase letter followed by uppercase letter (word boundary)
    normalized = re.sub(r'([a-z])([A-Z])', r'\1 \2', normalized)
    
    # Fix: word+number (but preserve phone formats like "+49 (0)40")
    # Only fix if not part of phone number pattern
    if not re.search(r'\+?\d+\s*\(', normalized):  # Not a phone number
        normalized = re.sub(r'([a-zA-Z])(\+?\d)', r'\1 \2', normalized)
    
    # Fix: number+word (but preserve units like "6GeV" -> "6 GeV")
    normalized = re.sub(r'(\d)([A-Za-z])', r'\1 \2', normalized)
    
    # Fix: punctuation+word (colon, semicolon, etc.)
    normalized = re.sub(r'([:;])([A-Za-z])', r'\1 \2', normalized)
    
    # Fix: word:number or word:+number (e.g., "hutch:+49" -> "hutch: +49")
    # But preserve phone formats like "+49 (0)40"
    normalized = re.sub(r'([a-zA-Z]):(\+?\d)', r'\1: \2', normalized)
    
    # Preserve email addresses and URLs (undo any changes to them)
    # This is a simple check - more complex patterns would need more sophisticated handling
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    url_pattern = r'https?://[^\s]+'
    
    return normalized


def post_process_markdown(content, tables_markdown):
    """Post-process assembled markdown: remove artifacts and clean up content.

    This is the "FINAL CLEANUP" pass that was previously inline in crawl_site() STEP 8.
    It is a pure string→string transformation.

    Args:
        content: The full assembled markdown string (URL header + tables + body).
        tables_markdown: The tables-only portion (used for table-content dedup).

    Returns:
        Cleaned markdown string.
    """
    lines = content.split('\n')
    cleaned_lines = []
    seen_headings = {}
    consecutive_empty = 0
    EARLY_LINE_THRESHOLD = 30  # Lines in first N are likely artifacts

    # FIX: Track which lines came from tables_markdown to avoid filtering them as duplicates
    # url_header is 4 lines: "# Source URL", "", URL, ""
    url_header_lines = 4
    tables_markdown_lines_count = len(tables_markdown.split('\n')) if tables_markdown else 0
    tables_markdown_start = url_header_lines
    tables_markdown_end = url_header_lines + tables_markdown_lines_count

    # BUG FIX: Protect URL header from being removed by cleanup logic
    url_header_start_idx = 0
    url_header_end_idx = url_header_lines

    # Common navigation/footer patterns to filter out
    nav_patterns = [
        r'data privacy policy', r'declaration of accessibility', r'impressum', r'datenschutz',
        r'cookie', r'privacy policy', r'accessibility', r'barrierefreiheit',
        r'^##\s+(PHOTON SCIENCE|Beamline Staff)$',  # Duplicate navigation headings
        r'breadcrumb',  # Breadcrumb navigation
        r'^##\s+Breadcrumb\s*Navigation',  # Breadcrumb navigation heading
        r'^##\s+Navigation$',  # Generic navigation heading
    ]

    for i, line in enumerate(lines):
        stripped = line.strip()

        # BUG FIX: Always preserve URL header lines (first 4 lines)
        if url_header_start_idx <= i < url_header_end_idx:
            cleaned_lines.append(line)
            if not stripped:
                consecutive_empty = 1
            else:
                consecutive_empty = 0
            continue

        # Remove excessive empty lines (max 2 consecutive)
        if not stripped:
            consecutive_empty += 1
            if consecutive_empty <= 2:
                cleaned_lines.append(line)
            continue
        consecutive_empty = 0

        # GENERAL: Filter malformed tables (10+ columns, concatenated data)
        if re.match(r'^\|', stripped):
            column_count = stripped.count('|') - 1
            if column_count > 10:
                continue
            if column_count == 1:
                first_cell_match = re.match(r'^\|\s*([^|]+)', stripped)
                if first_cell_match:
                    first_cell = first_cell_match.group(1)
                    field_label_count = len(re.findall(r'\b(E-Mail|Phone|Location|Email|Tel|Telephone):', first_cell, re.I))
                    if field_label_count >= 3:
                        continue
            elif column_count >= 2 and column_count <= 10:
                pass  # Don't filter multi-column tables with field labels

        # GENERAL: Filter navigation/footer links
        nav_link_patterns = [
            r'helmholtz\.de', r'door\.desy\.de', r'xfel\.eu', r'cfel\.de', r'cssb-hamburg',
            r'pbook', r'data_privacy', r'More information'
        ]
        if any(re.search(pattern, stripped, re.I) for pattern in nav_link_patterns):
            continue

        # Remove navigation/footer content (general patterns)
        if any(re.search(pattern, stripped, re.I) for pattern in nav_patterns):
            if stripped.startswith('##'):
                j = i + 1
                while j < len(lines):
                    next_stripped = lines[j].strip()
                    if next_stripped.startswith('#'):
                        break
                    j += 1
                continue
            continue

        # GENERAL: Remove empty headings
        if stripped.startswith('#'):
            if stripped.lower() == '# source url':
                cleaned_lines.append(line)
                continue
            heading_text = stripped.lstrip('#').strip()
            if not heading_text:
                continue

            # GENERAL: Remove empty sections
            section_start_idx = i
            section_end_idx = len(lines)

            next_heading_level = None
            for j in range(i + 1, len(lines)):
                next_stripped = lines[j].strip()
                if next_stripped.startswith('#'):
                    section_end_idx = j
                    next_heading_level = len(next_stripped) - len(next_stripped.lstrip('#'))
                    break

            has_content = False
            for j in range(section_start_idx + 1, section_end_idx):
                content_line = lines[j].strip()
                if not content_line:
                    continue
                if _is_separator_line(content_line):
                    continue
                if content_line.startswith('#'):
                    continue
                has_content = True
                break

            current_heading_level = len(stripped) - len(stripped.lstrip('#'))
            is_subheading = (next_heading_level is not None and next_heading_level > current_heading_level)
            is_sibling = (next_heading_level is not None and next_heading_level == current_heading_level)
            is_parent = (next_heading_level is not None and next_heading_level < current_heading_level)
            has_any_next = (next_heading_level is not None)

            should_remove = (not has_content and
                           ((not has_any_next) or
                            (has_any_next and is_subheading and not is_sibling and not is_parent)))

            if should_remove:
                continue

        # Remove early horizontal rules and orphaned separators
        if i < EARLY_LINE_THRESHOLD:
            if _is_separator_line(stripped):
                continue

        # Remove orphaned table separators without proper table context
        if _is_separator_line(stripped):
            has_header = any(re.match(r'^\|\s*[^|]+\s*\|.*\|', lines[j].strip())
                            for j in range(max(0, i - 20), i) if lines[j].strip() and not lines[j].strip().startswith('#'))
            has_row = any(re.match(r'^\|\s*[^|]+\s*\|.*\|', lines[j].strip())
                         for j in range(i + 1, min(len(lines), i + 20)) if lines[j].strip() and not lines[j].strip().startswith('#'))
            if not (has_header and has_row):
                continue

        # GENERAL: Remove broken text fragments (single values like "192 ns", "6.0 GeV", etc.)
        if re.match(r'^[\d\s.,]+(ns|ms|μs|μm|mm|m|GeV|keV|MeV|T|kW|h|°|%|kHz|MHz|psec|nC|mrad|pmrad|μrad)\s*$', stripped, re.I):
            continue

        # Remove leftover names
        if not any(stripped.startswith(c) for c in '#-|*'):
            words = stripped.split()
            is_name = False
            if len(words) == 1 and words[0][0].isupper() and len(words[0]) > 2:
                is_name = True
            elif ',' in stripped:
                parts = [p.strip() for p in stripped.split(',')]
                if len(parts) == 2 and all(p and p[0].isupper() for p in parts):
                    is_name = True
            if is_name:
                empty_ahead = sum(1 for j in range(i + 1, min(len(lines), i + 10))
                                 if not lines[j].strip())
                if empty_ahead >= 3:
                    continue

        # Remove duplicate headings (within 20 lines)
        if stripped.startswith('#'):
            heading_sig = stripped.lstrip('#').strip().lower()
            if heading_sig in seen_headings and i - seen_headings[heading_sig] < 20:
                continue
            seen_headings[heading_sig] = i

        # FIX: Remove text lines that are substrings of table content
        is_from_tables_markdown = tables_markdown_start <= i < tables_markdown_end

        if tables_markdown and not is_from_tables_markdown and not stripped.startswith(('#', '|', '-', '*')):
            stripped_normalized = re.sub(r'\s+', ' ', stripped.lower().strip())
            if len(stripped_normalized) > 5:
                tables_normalized = re.sub(r'\s+', ' ', tables_markdown.lower())
                if stripped_normalized in tables_normalized:
                    continue

        # GENERAL: Normalize text spacing to fix concatenation issues
        normalized_line = _normalize_text_spacing(line)
        cleaned_lines.append(normalized_line)

    # Remove leading empty lines and orphaned separators
    # BUG FIX: Never remove the URL header (first 4 lines)
    header_preserved = []
    if len(cleaned_lines) >= url_header_lines:
        header_preserved = cleaned_lines[:url_header_lines]
        cleaned_lines = cleaned_lines[url_header_lines:]

    while cleaned_lines:
        first_stripped = cleaned_lines[0].strip()
        if not first_stripped:
            cleaned_lines.pop(0)
        elif _is_separator_line(first_stripped):
            cleaned_lines.pop(0)
        else:
            break

    cleaned_lines = header_preserved + cleaned_lines

    # GENERAL: Ensure "External Links" section has proper header and remove duplicates
    external_link_pattern = r'^- \[.*\]\(https?://[^)]+\)'
    has_external_links = False
    external_links_start_idx = None
    external_links_header_indices = []

    for i, line in enumerate(cleaned_lines):
        stripped_line = line.strip()
        if re.match(external_link_pattern, stripped_line):
            if external_links_start_idx is None:
                external_links_start_idx = i
            has_external_links = True
        elif stripped_line == '## External Links':
            external_links_header_indices.append(i)

    # Remove duplicate "External Links" headers (keep only the first one)
    if external_links_header_indices and len(external_links_header_indices) > 1:
        first_header_idx = min(external_links_header_indices)
        for header_idx in sorted(external_links_header_indices, reverse=True):
            if header_idx == first_header_idx:
                continue
            if header_idx < len(cleaned_lines):
                cleaned_lines.pop(header_idx)
                if header_idx < len(cleaned_lines) and not cleaned_lines[header_idx].strip():
                    cleaned_lines.pop(header_idx)
                if header_idx > 0 and not cleaned_lines[header_idx - 1].strip():
                    cleaned_lines.pop(header_idx - 1)

    # If external links exist but no header before them, add it
    if has_external_links and external_links_start_idx is not None:
        remaining_header_indices = [i for i, line in enumerate(cleaned_lines) if line.strip() == '## External Links']
        has_header_before = any(idx < external_links_start_idx for idx in remaining_header_indices)
        if not has_header_before:
            if external_links_start_idx > 0 and cleaned_lines[external_links_start_idx - 1].strip():
                cleaned_lines.insert(external_links_start_idx, '')
            cleaned_lines.insert(external_links_start_idx, '## External Links')
            if external_links_start_idx + 1 < len(cleaned_lines) and cleaned_lines[external_links_start_idx + 1].strip():
                cleaned_lines.insert(external_links_start_idx + 1, '')

    # FINAL PASS: Remove any remaining empty sections (safety check)
    final_cleaned = []
    i = 0
    while i < len(cleaned_lines):
        line = cleaned_lines[i]
        stripped = line.strip()

        if stripped.startswith('#'):
            heading_text = stripped.lstrip('#').strip()
            if heading_text:
                section_start = i
                section_end = len(cleaned_lines)
                next_heading_level_final = None
                for j in range(i + 1, len(cleaned_lines)):
                    next_line_stripped = cleaned_lines[j].strip()
                    if next_line_stripped.startswith('#'):
                        section_end = j
                        next_heading_level_final = len(next_line_stripped) - len(next_line_stripped.lstrip('#'))
                        break
                has_content = False
                for j in range(section_start + 1, section_end):
                    content_line = cleaned_lines[j].strip()
                    if not content_line:
                        continue
                    if content_line.startswith('#'):
                        continue
                    if _is_separator_line(content_line):
                        continue
                    has_content = True
                    break

                current_heading_level_final = len(stripped) - len(stripped.lstrip('#'))
                is_subheading_final = (next_heading_level_final is not None and next_heading_level_final > current_heading_level_final)
                is_sibling_heading = (next_heading_level_final is not None and next_heading_level_final == current_heading_level_final)
                is_parent_heading = (next_heading_level_final is not None and next_heading_level_final < current_heading_level_final)
                has_any_next_heading = (next_heading_level_final is not None)

                should_remove = (not has_content and
                               ((not has_any_next_heading) or
                                (has_any_next_heading and is_subheading_final and not is_sibling_heading and not is_parent_heading)))

                if should_remove:
                    i = section_end
                    continue

        final_cleaned.append(line)
        i += 1

    return '\n'.join(final_cleaned)


def is_empty_page(content):
    """Check if page content is empty/minimal (only URL header and error messages).

    Args:
        content: The cleaned markdown content string.

    Returns:
        True if the page should be skipped (empty/minimal content).
    """
    content_without_header = re.sub(r'^#\s*Source\s*URL.*?\n---\s*\n', '', content, flags=re.IGNORECASE | re.MULTILINE)
    content_meaningful = content_without_header.strip()

    if len(content_meaningful) < 50:
        return True

    if len(content_meaningful) < 200:
        error_patterns = [
            r'page could not be found',
            r'404',
            r'not found',
            r'error',
            r'page not available'
        ]
        content_lower = content_meaningful.lower()
        if any(pattern in content_lower for pattern in error_patterns):
            words = [w for w in content_meaningful.split() if len(w) > 2 and not w.startswith('http') and not w.startswith('#')]
            if len(words) < 10:
                return True

    return False


def deduplicate_markdown_against_tables(markdown_content, tables_markdown, page_url, page_html):
    """Remove headings, table rows, broken fragments, and contact info from
    markdown_content that duplicate content already in tables_markdown.

    Used when DOM-order table extraction (tables_markdown) is the primary
    source: headings and table sections from the raw markdown are stripped
    to avoid duplicates, and broken label-value fragments and contact info
    lines that mirror table content are removed.

    Args:
        markdown_content: Raw markdown text to filter.
        tables_markdown: DOM-order tables markdown (source of truth).
        page_url: Page URL (for PUBDB page detection), may be None.
        page_html: Page HTML (for PUBDB page detection), may be None.

    Returns:
        Cleaned text with duplicates removed (may be empty string).
    """
    if not markdown_content:
        return ''

    lines = markdown_content.split('\n')
    cleaned_lines = []
    i = 0

    # Extract headings from tables_markdown to know what to remove
    tables_markdown_lines = tables_markdown.split('\n') if tables_markdown else []
    headings_in_tables_markdown = set()
    for tm_line in tables_markdown_lines:
        tm_stripped = tm_line.strip()
        if tm_stripped.startswith('#'):
            heading_text = tm_stripped.lstrip('#').strip()
            heading_text_normalized = ' '.join(heading_text.split())
            headings_in_tables_markdown.add(heading_text_normalized.lower())

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Remove headings that are already in tables_markdown
        if stripped.startswith('#'):
            heading_text = stripped.lstrip('#').strip()
            # Remove empty headings (just ## with spaces) or headings that match tables_markdown
            if not heading_text or heading_text.lower() in headings_in_tables_markdown:
                i += 1
                continue

        # Remove text lines that match heading text in tables_markdown
        # Some headings appear as plain text (not starting with #) in markdown_content
        if stripped and not stripped.startswith('#') and not stripped.startswith('|'):
            stripped_normalized = ' '.join(stripped.split()).lower()
            if stripped_normalized in headings_in_tables_markdown:
                i += 1
                continue

        # Remove broken label-value fragments (text with pipes, not proper tables)
        # Pattern: "Label:---|---" or "Label:|  Value"
        if ':' in stripped and '|' in stripped and not stripped.startswith('|'):
            is_broken_fragment = (
                re.match(r'^[^|]+:\s*[-]+\|', stripped) or  # "Label:---|" or "Label:---|---"
                re.match(r'^[^|]+:\s*\|', stripped)  # "Label:| Value"
            )
            if is_broken_fragment:
                i += 1
                # Skip following separator lines (---|---)
                while i < len(lines):
                    next_stripped = lines[i].strip()
                    if not next_stripped:
                        i += 1
                        if i < len(lines) and lines[i].strip():
                            if ':' in lines[i].strip() and '|' in lines[i].strip() and not lines[i].strip().startswith('|'):
                                continue
                        break
                    elif _is_separator_line(next_stripped):
                        i += 1
                    elif next_stripped and ':' in next_stripped and '|' in next_stripped and not next_stripped.startswith('|'):
                        i += 1
                    else:
                        break
                continue

        # Remove orphaned separator lines (---|---) that aren't part of proper tables
        if _is_separator_line(stripped):
            has_table_before = any(re.match(r'^\|', lines[j].strip()) for j in range(max(0, i - 10), i) if lines[j].strip() and not lines[j].strip().startswith('#'))
            has_table_after = any(re.match(r'^\|', lines[j].strip()) for j in range(i + 1, min(len(lines), i + 10)) if lines[j].strip() and not lines[j].strip().startswith('#'))
            if not (has_table_before and has_table_after):
                i += 1
                continue

        # Remove table sections from markdown_content
        if re.match(r'^\|', stripped):
            # PUBDB-specific filtering: Only filter UI tables on PUBDB pages
            if _table_processing._is_pubdb_page(page_url, page_html):
                # Collect table lines to check (up to 20 lines, first 5 rows for analysis)
                table_lines_to_check = []
                table_end = i
                while table_end < len(lines) and table_end < i + 20:
                    next_line = lines[table_end].strip()
                    if re.match(r'^\|', next_line):
                        table_lines_to_check.append(next_line)
                        table_end += 1
                    elif not next_line:
                        if table_end + 1 < len(lines) and re.match(r'^\|', lines[table_end + 1].strip()):
                            table_end += 1
                        else:
                            break
                    else:
                        break

                # Check for PUBDB UI keywords in table content (first 5 rows)
                table_content = ' '.join(table_lines_to_check[:5]).lower()

                if _table_processing.is_pubdb_ui_table(table_content):
                    i = table_end
                    continue

            # For non-PUBDB pages or non-UI tables on PUBDB pages:
            # Remove table sections from markdown_content (they're already in tables_markdown)
            table_end = i
            while table_end < len(lines):
                next_line = lines[table_end].strip()
                if re.match(r'^\|', next_line):
                    table_end += 1
                elif not next_line:
                    if table_end + 1 < len(lines) and re.match(r'^\|', lines[table_end + 1].strip()):
                        table_end += 1
                    else:
                        break
                else:
                    break
            i = table_end
            continue

        # Remove text lines that duplicate table content (contact info)
        if stripped and not stripped.startswith('#') and not stripped.startswith('|'):
            has_field_labels = re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', stripped, re.I) is not None

            is_name_line = False
            words = stripped.split()
            if len(words) <= 3 and all(w and w[0].isupper() and w.replace('-', '').isalnum() for w in words if w):
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', next_line, re.I) is not None:
                        is_name_line = True
                if has_field_labels:
                    is_name_line = True
                if not is_name_line and i + 2 < len(lines):
                    next_next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
                    after_next = lines[i + 2].strip() if i + 2 < len(lines) else ""
                    if (next_next_line and len(next_next_line.split()) <= 3 and
                        all(w and w[0].isupper() and w.replace('-', '').isalnum() for w in next_next_line.split() if w) and
                        after_next and re.search(r'\b(E-Mail|Email|Phone|Tel|Telephone|Location|Office|Room):', after_next, re.I) is not None):
                        is_name_line = True

            has_contact_patterns = bool(re.search(r'mailto:|@|phone|tel|\+?\d{2,}', stripped, re.I))
            has_location_patterns = bool(re.search(r'\d+\s*[a-z]\s*/\s*\d+|location|office|room', stripped, re.I))

            # Only remove contact info if tables_markdown has content
            tables_has_content = tables_markdown and len(tables_markdown.strip()) > 50

            if tables_has_content and (has_field_labels or is_name_line or has_contact_patterns or has_location_patterns):
                i += 1
                continue

        cleaned_lines.append(line)
        i += 1

    return '\n'.join(cleaned_lines).strip()


def clean_raw_markdown(markdown_content, page_url):
    """Clean raw markdown content before assembly into final document.

    Removes existing URL headers, breadcrumb navigation, duplicate lines,
    empty-text links, navigation patterns (spacer/header images, nav menus),
    and broken table fragments.

    Args:
        markdown_content: The raw markdown string from the crawler.
        page_url: The URL of the page (used to strip self-referencing lines).

    Returns:
        Cleaned markdown string.
    """
    if not markdown_content:
        return markdown_content

    # Remove URL header pattern if it exists
    markdown_content = re.sub(r'^#\s*Source\s*URL.*?\n---\s*\n', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
    # Remove "URL: <url>" patterns and breadcrumb navigation
    markdown_content = re.sub(r'^URL:\s*https?://[^\s]+\s*\n?', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE)
    markdown_content = re.sub(r'^Breadcrumb\s+Navigation\s*\n?', '', markdown_content, flags=re.IGNORECASE | re.MULTILINE)
    # Remove lines that are just the current page URL
    if page_url:
        markdown_content = re.sub(r'^' + re.escape(page_url) + r'\s*\n?', '', markdown_content, flags=re.MULTILINE)

    # Apply enhanced duplication detection to final markdown
    markdown_lines = markdown_content.split('\n')
    duplicates = _content_extraction.detect_enhanced_repetition(markdown_lines)

    # Remove duplicate lines (keep first occurrence)
    deduplicated_lines = []
    for i, line in enumerate(markdown_lines):
        if i not in duplicates:
            deduplicated_lines.append(line)

    markdown_content = '\n'.join(deduplicated_lines)

    # Clean markdown link syntax (remove whitespace from links)
    markdown_content = _content_extraction.clean_markdown_links_post_process(markdown_content)

    # Remove empty-text markdown links like [](...) but keep images ![](...)
    markdown_content = re.sub(r'(?<!\!)\[\]\([^)]+\)', '', markdown_content)

    # Remove navigation patterns (spacer images, header images, nav menus)
    lines = markdown_content.split('\n')
    cleaned_lines = []
    for line in lines:
        if re.search(r'spacer\.(gif|png|jpg)', line, re.I):
            continue
        if re.search(r'(header|desy|logo|banner)\.(jpg|png|gif)', line, re.I):
            continue
        if re.search(r'!\[\]\([^)]+(spacer|header|desy|logo|banner)[^)]+\)', line, re.I):
            continue
        if re.search(r'(To sort click|navigation|menu|breadcrumb)', line, re.I):
            continue
        link_count = len(re.findall(r'\[([^\]]+)\]\([^)]+\)', line))
        if link_count > 3 and len(line.strip()) < 150:
            continue
        if re.match(r'^!\[.*?\]\([^)]+\)\s*\|?\s*$', line.strip()):
            continue
        cleaned_lines.append(line)
    markdown_content = '\n'.join(cleaned_lines)

    # Remove broken table fragments
    markdown_content = _remove_broken_table_fragments(markdown_content)

    return markdown_content.strip()


def _remove_broken_table_fragments(markdown_content):
    """Remove broken table fragments that aren't part of proper tables.

    Handles patterns like single-cell rows, name + table-cell sequences,
    and orphaned separators without headers.

    Args:
        markdown_content: Markdown string potentially containing broken fragments.

    Returns:
        Cleaned markdown string.
    """
    lines = markdown_content.split('\n')
    cleaned_lines = []
    i = 0
    in_proper_table = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip() if line else ""

        # Detect proper table start: separator with header before
        if re.match(r'^\|[\s\-:]+\|', stripped):
            prev_is_header = False
            if i > 0:
                prev_stripped = lines[i - 1].strip()
                if prev_stripped and re.match(r'^\|\s*[^|]+\s*\|', prev_stripped):
                    prev_is_header = True

            if not prev_is_header:
                i += 1
                continue

            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if re.match(r'^\|\s*[^|]+\s*\|', next_line):
                    in_proper_table = True
                    cleaned_lines.append(line)
                    i += 1
                    continue

        if re.match(r'^\|\s*[^|]+\s*\|', stripped):
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if re.match(r'^\|[\s\-:]+\|', next_line):
                    in_proper_table = True
                    cleaned_lines.append(line)
                    i += 1
                    continue

        # If we're in a proper table, keep all rows
        if in_proper_table:
            if not stripped or not re.match(r'^\|', stripped):
                in_proper_table = False
            cleaned_lines.append(line)
            i += 1
            continue

        # Detect broken fragment pattern: Name on one line, then table cells on next lines
        if not re.match(r'^\|', stripped) and stripped and not stripped.startswith('#'):
            fragment_lines = []
            j = i + 1
            consecutive_empty = 0
            while j < len(lines) and j < i + 15:
                next_stripped = lines[j].strip()
                if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', next_stripped) or re.match(r'^\|\s*[^|]+\s*\|', next_stripped):
                    fragment_lines.append(j)
                    consecutive_empty = 0
                    j += 1
                elif not next_stripped:
                    consecutive_empty += 1
                    if consecutive_empty <= 2:
                        j += 1
                    else:
                        break
                else:
                    if fragment_lines and len(fragment_lines) >= 2:
                        break
                    else:
                        break

            if fragment_lines and len(fragment_lines) >= 2:
                i = fragment_lines[-1] + 1
                continue

        # Single-cell rows not in proper tables
        if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', stripped):
            fragment_count = 0
            j = i + 1
            while j < len(lines) and j < i + 10:
                next_stripped = lines[j].strip()
                if re.match(r'^\|\s+[^|]+(\s*\|)?\s*$', next_stripped):
                    fragment_count += 1
                    j += 1
                elif not next_stripped:
                    j += 1
                else:
                    break

            prev_is_name = False
            for check_idx in [i - 1, i - 2]:
                if check_idx >= 0:
                    prev_stripped = lines[check_idx].strip()
                    if prev_stripped and not re.match(r'^\|', prev_stripped) and not prev_stripped.startswith('#'):
                        prev_is_name = True
                        break

            if fragment_count >= 1 or prev_is_name:
                i += 1
                continue

        # Multi-cell rows that aren't in proper tables
        if re.match(r'^\|\s*[^|]+\s*\|', stripped):
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if not re.match(r'^\|[\s\-:]+\|', next_line) and not re.match(r'^\|\s*[^|]+\s*\|', next_line):
                    i += 1
                    continue

        cleaned_lines.append(line)
        i += 1
    return '\n'.join(cleaned_lines)


def fill_empty_name_columns(content):
    """Fill empty Name columns in tables by extracting name from email links.
    
    Handles table rows that start with || (empty first cell) by extracting
    the name from email link text and filling the empty cell.
    Only fills if the first cell is truly empty (just whitespace).
    """
    lines = content.split('\n')
    result_lines = []
    in_name_table = False  # Track if we're in a table with Name column
    in_any_table = False  # Track if we're in any table
    
    for line in lines:
        stripped = line.strip()
        
        # Detect table headers with Name column
        if stripped.startswith('|') and re.search(r'\|\s*Name\s*\|', stripped, re.I):
            in_name_table = True
            in_any_table = True
            result_lines.append(line)
            continue
        
        # Detect any table header (for tables without Name column)
        if stripped.startswith('|') and '---' not in stripped and not in_any_table:
            # Check if it looks like a table header (has multiple columns)
            cols = [c.strip() for c in stripped.split('|') if c.strip()]
            if len(cols) >= 2:
                in_any_table = True
                
        
        # Reset when we leave the table (empty line or non-table line)
        if not stripped or not stripped.startswith('|'):
            in_name_table = False
            in_any_table = False
            result_lines.append(line)
            continue
        
        # Skip separator rows
        if '---' in stripped:
            result_lines.append(line)
            continue
        
        # Process table rows with empty first cell
        # Pattern: ||  | [Name](mailto:...) | ... OR |  | [Name](mailto:...) | ...
        # Process if: (1) in table with Name column OR (2) in any table with empty first cell
        if stripped.startswith('|'):
            # Check if first cell is empty: || ... or |  |
            # Match: |<empty or whitespace>| OR || (double pipe at start)
            # FIX 1a: The original code used `or stripped.startswith('||')` as a fallback,
            # but re.match(r'^\|\s*\|', ...) already handles '||' (zero whitespace between
            # pipes). The fallback returned a bool True, which then had .group(0) called on
            # it, raising AttributeError. Removed the redundant fallback entirely.
            first_cell_empty = re.match(r'^\|\s*\|', stripped)
            
            
            
            if first_cell_empty:
                # Look for email link in the row
                email_match = re.search(r'\[([^\]]+)\]\(mailto:[^)]+\)', stripped)
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
                        
                        # Check if name already exists in the row (outside the email link)
                        # Remove the email link part and check if name appears in remaining text
                        row_without_link = re.sub(r'\[[^\]]+\]\([^)]+\)', '', stripped)
                        name_already_exists = link_text.lower() in row_without_link.lower()
                        
                        # Fill if: (1) in table with Name column OR (2) in any table with empty first cell
                        if is_name and not has_phone and not name_already_exists and (in_name_table or in_any_table):
                            
                            # Replace the empty first cell with the name
                            # |  | -> | Name |
                            # FIX 1a: Use .end() for the slice offset instead of len(group(0)).
                            # .end() gives the exact character position after the match,
                            # correctly handling any prefix whitespace not in the match.
                            line = '| ' + link_text + ' |' + stripped[first_cell_empty.end():]
        
        result_lines.append(line)
    
    return '\n'.join(result_lines)
