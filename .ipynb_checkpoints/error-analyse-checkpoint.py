import json

with open('desy_crawled/crawl_errors.json', 'r') as f:
    errors = json.load(f)

print("=== ERROR ANALYSIS ===")
print(f"Total errors: {errors['total_errors']}")
print(f"Total successful: {errors['total_successful']}\n")

# Categorize errors
timeout_errors = []
indico_ics_errors = []
connection_errors = []
other_errors = []

for error in errors['errors']:
    url = error['url']
    error_msg = error['error']
    
    if 'Timeout' in error_msg or 'timeout' in error_msg.lower():
        timeout_errors.append(url)
    elif '.ics' in url or 'indico' in url.lower():
        indico_ics_errors.append(url)
    elif 'ERR_NAME_NOT_RESOLVED' in error_msg or 'ERR_CONNECTION_REFUSED' in error_msg:
        connection_errors.append(url)
    else:
        other_errors.append(url)

print(f"Timeout errors: {len(timeout_errors)}")
if timeout_errors:
    print(f"  Examples: {timeout_errors[:3]}")

print(f"\nIndico .ics errors: {len(indico_ics_errors)}")
if indico_ics_errors:
    print(f"  Examples: {indico_ics_errors[:3]}")

print(f"\nConnection errors: {len(connection_errors)}")
if connection_errors:
    print(f"  Examples: {connection_errors[:3]}")

print(f"\nOther errors: {len(other_errors)}")

