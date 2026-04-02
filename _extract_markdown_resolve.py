"""Extract _resolve_markdown_content() from crawl_site() lines 2430-3222.

Moves the large markdown content resolution block (G6+G7: fit/raw selection +
HTML fallback extraction) into a standalone function.

Usage: python3 _extract_markdown_resolve.py
"""
import re, sys

FILE = "crawl_desy_all_urls.py"

with open(FILE) as f:
    lines = f.readlines()

# ── Locate block boundaries ────────────────────────────────────────
# Start: line with 'markdown_content = ""' (the first one inside try, at indent 20)
# End: line after 'markdown_content = result.markdown or ""' (the else branch)
#       The next line is blank, then '# Post-process markdown to inject links'

block_start = None
for i, line in enumerate(lines):
    if 'markdown_content = ""' in line and line.strip() == 'markdown_content = ""':
        block_start = i
        break
assert block_start is not None, "Could not find markdown_content = '' line"

# tables_markdown = "" is one line below markdown_content = ""
tables_init_line = block_start + 1
assert 'tables_markdown = ""' in lines[tables_init_line], f"Expected tables_markdown init at line {tables_init_line+1}, got: {lines[tables_init_line].strip()}"

# Find the end: "# Post-process markdown to inject links"
block_end = None
for i in range(block_start + 1, len(lines)):
    if '# Post-process markdown to inject links' in lines[i]:
        block_end = i
        break
assert block_end is not None, "Could not find post-process marker"

print(f"Block to extract: lines {block_start+1}-{block_end} ({block_end - block_start} lines)")

# ── Measure indentation ────────────────────────────────────────────
# The block is at 20-space indent inside crawl_site
base_indent = 20  # spaces
new_indent = 4    # function body indent
dedent = base_indent - new_indent

# ── Build function body ────────────────────────────────────────────
func_body_lines = []
for i in range(block_start, block_end):
    line = lines[i]
    # Dedent the line
    if line.strip() == '':
        func_body_lines.append('\n')
    else:
        # Count leading spaces
        stripped = line.lstrip(' ')
        leading = len(line) - len(stripped)
        new_leading = max(0, leading - dedent)
        func_body_lines.append(' ' * new_leading + stripped)

# Remove trailing blank lines
while func_body_lines and func_body_lines[-1].strip() == '':
    func_body_lines.pop()

# Add return statement
func_body_lines.append('\n')
func_body_lines.append('    return markdown_content\n')

func_body = ''.join(func_body_lines)

# ── Build the function definition ──────────────────────────────────
func_def = '''
def _resolve_markdown_content(result, result_is_pdf):
    """Select best markdown from fit/raw, falling back to HTML extraction.

    Encapsulates Steps G6-G7 of crawl_site(): fit-vs-raw markdown selection,
    table detection, HTML fallback via BeautifulSoup (link conversion, contact
    block merging, paragraph extraction, noise filtering, dedup), and
    full-page last-resort extraction.

    Returns the resolved markdown content string (may be empty).
    """
'''

full_function = func_def + func_body + '\n\n'

# ── Build replacement call site ────────────────────────────────────
# We keep:
#   line block_start:   markdown_content = ""
#   line tables_init_line: tables_markdown = ""  # Initialize ...
# Then a call to the new function (overwriting markdown_content if content found)
call_indent = ' ' * base_indent
replacement = (
    call_indent + 'markdown_content = ""\n' +
    lines[tables_init_line] +  # tables_markdown = "" line (already correct indent)
    call_indent + 'if hasattr(result, "markdown") or (hasattr(result, "html") and result.html):\n' +
    call_indent + '    markdown_content = _resolve_markdown_content(result, result_is_pdf)\n'
)

# ── Find insertion point for the function ──────────────────────────
# Insert before async def crawl_site(), after _print_crawl_summary
crawl_site_def = None
for i, line in enumerate(lines):
    if line.strip() == 'async def crawl_site():':
        crawl_site_def = i
        break
assert crawl_site_def is not None

# ── Assemble new file ──────────────────────────────────────────────
new_lines = []
# 1. Everything up to crawl_site()
new_lines.extend(lines[:crawl_site_def])
# 2. The new function
new_lines.append(full_function)
# 3. crawl_site() up to but NOT including block_start (markdown_content = "")
new_lines.extend(lines[crawl_site_def:block_start])
# 4. The replacement call (includes markdown_content = "", tables_markdown = "", + call)
new_lines.append(replacement)
# 5. Rest of file from block_end onwards
new_lines.extend(lines[block_end:])

with open(FILE, 'w') as f:
    f.writelines(new_lines)

old_count = len(lines)
new_count = len(new_lines)
print(f"Done! {old_count} -> {new_count} lines")
print(f"  Extracted lines {block_start+1}-{block_end} into _resolve_markdown_content()")
print(f"  Net change: {new_count - old_count:+d} lines")
