import os
import re
import time
import random
import gzip
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — the only section you need to edit between runs
# ══════════════════════════════════════════════════════════════════════════════

# --- EPISODE AND OUTPUT SETTINGS ---
EPISODE     = "bitkeeper_episode"   # One folder for the entire episode
OUTPUT_BASE = r"C:\Users\pedra\Desktop"

# --- DATE CHUNKS ---
# The full episode spans 2002–2005: from BitKeeper's adoption by the Linux
# kernel project through the controversy over Andrew Tridgell's reverse
# engineering attempt, BitMover's license revocation, and the creation of Git.
# We break the window into one-year chunks purely for technical reasons
# (server load, manageable run sizes).
# These boundaries carry NO analytical meaning — do not interpret them
# as sub-episodes. Periodization will emerge from the data.
DATE_CHUNKS = [
    ("2002-01-01", "2002-12-31"),
    ("2003-01-01", "2003-12-31"),
    ("2004-01-01", "2004-12-31"),
    ("2005-01-01", "2005-12-31"),
]

# --- QUERY ---
# A single compound boolean query submitted directly to lore.kernel.org.
# Each term targets a distinct facet of the BitKeeper controversy:
# the tool itself, key actors, the reverse-engineering incident, and
# the proprietary-licence debate that ultimately led to Git's creation.
BLOB_TERMS = [
    '"bitkeeper linux"',
    '"bk kernel"',
    '"larry mcvoy"',
    '"tridgell bitkeeper"',
    '"bitmover"',
    '"reverse engineer bitkeeper"',
    '"sourcepuller"',
    '"bitkeeper protocol"',
    '"revoke bitkeeper"',
    '"proprietary bitkeeper"',
]

CONTROVERSY_TERMS = [
    'secrecy',
    'transparency',
    '"black box"',
    '"trust the vendor"',
    '"vendor trust"',
    'opaque',
    'auditable',
    '"security through obscurity"',
    'flame',
    'controversy',
    '"holy war"',
]

# Assembled query (date range is injected per chunk at runtime — see build_query)
QUERY_BLOB        = "(" + " OR ".join(BLOB_TERMS) + ")"
QUERY_CONTROVERSY = "(" + " OR ".join(CONTROVERSY_TERMS) + ")"
QUERY_BASE        = f"{QUERY_BLOB} AND {QUERY_CONTROVERSY}"

MAX_THREADS = 300   # Per date chunk
DELAY_MIN   = 2     # Minimum seconds between requests — do not set below 2
DELAY_MAX   = 5     # Maximum seconds between requests

# ══════════════════════════════════════════════════════════════════════════════
# SOURCE SETTINGS — edit this section when adapting for a new data source
# ══════════════════════════════════════════════════════════════════════════════

SEARCH_URL = "https://lore.kernel.org/lkml/"

def build_query(date_from, date_to):
    """
    Append the date range to the compound boolean query.
    Example: ("bitkeeper linux" OR ...) AND (secrecy OR ...) d:20020101..20021231
    """
    d_from = date_from.replace("-", "")
    d_to   = date_to.replace("-", "")
    return f"{QUERY_BASE} d:{d_from}..{d_to}"

def build_search_url(query, offset):
    return f"{SEARCH_URL}?q={requests.utils.quote(query)}&o={offset}"

def extract_msg_ids_from_page(soup):
    msg_ids = []
    for pre in soup.find_all("pre"):
        for a_tag in pre.find_all("a", href=True):
            href = a_tag["href"]
            if any(x in href for x in ["?", "#", ".css", "_/", "http", "mirror", "help", "color"]):
                continue
            if not re.match(r'^[A-Za-z0-9$._@!%+\-]+/$', href):
                continue
            msg_ids.append(href.rstrip("/"))
    return msg_ids

def build_message_url(msg_id):
    return f"{SEARCH_URL}{msg_id}/"

def build_download_url(msg_id):
    return f"{SEARCH_URL}{msg_id}/t.mbox.gz"

def extract_date_from_msg_id(msg_id):
    match = re.match(r'^(20\d{2})(\d{2})(\d{2})\d+', msg_id)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None

PAGE_STEP = 200

# ══════════════════════════════════════════════════════════════════════════════
# CORE FUNCTIONS — do not edit these when adapting for a new source
# ══════════════════════════════════════════════════════════════════════════════

def slugify(text):
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text[:60]

def date_in_range(date_str, date_from, date_to):
    if not date_str:
        return None
    return date_from <= date_str <= date_to

def get_subject_from_page(msg_url, page):
    try:
        page.goto(msg_url, wait_until="domcontentloaded", timeout=30000)
        wait_if_challenged(page)
        time.sleep(random.uniform(1, DELAY_MIN))
        soup      = BeautifulSoup(page.content(), "html.parser")
        title_tag = soup.find("title")
        subject   = title_tag.get_text(strip=True) if title_tag else "untitled"
        subject   = re.sub(r'\s*-\s*lore\.kernel\.org.*$', '', subject, flags=re.I)
        return subject.strip()
    except Exception as e:
        print(f"    WARNING: could not get subject: {e}")
        return "untitled"

def flag_matching_messages(thread_text):
    """
    Flag individual messages within a thread that contain any blob term
    or any controversy term, noting which matched.
    """
    blob_plain        = [t.strip('"') for t in BLOB_TERMS]
    controversy_plain = [t.strip('"') for t in CONTROVERSY_TERMS]

    messages = thread_text.split("\nFrom ")
    flagged  = []

    for message in messages:
        msg_lower   = message.lower()
        blob_hits   = [t for t in blob_plain        if t.lower() in msg_lower]
        contro_hits = [t for t in controversy_plain if t.lower() in msg_lower]

        if blob_hits or contro_hits:
            lines     = message.split("\n", 1)
            flag_line = (
                f"X-Dissertation-Match: KEYWORD_MATCH "
                f"(blob: {', '.join(blob_hits) or 'none'}; "
                f"controversy: {', '.join(contro_hits) or 'none'})\n"
            )
            message = lines[0] + "\n" + flag_line + lines[1] if len(lines) == 2 \
                      else message + "\n" + flag_line

        flagged.append(message)

    return "\nFrom ".join(flagged)

def wait_if_challenged(page):
    """
    Detect robot/CAPTCHA challenge pages and pause for manual solving.
    Called after every page.goto() throughout the script.
    Challenge indicators: no <pre> tags (search results) or known challenge
    keywords in the page title or body.
    """
    try:
        html      = page.content()
        page_low  = html.lower()
        title_tag = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
        title     = title_tag.group(1).lower() if title_tag else ""

        challenge_signals = [
            "robot", "captcha", "challenge", "ddos", "cloudflare",
            "access denied", "forbidden", "are you human", "verify you are"
        ]
        is_challenge = any(s in title or s in page_low[:2000] for s in challenge_signals)

        # Also flag if lore search page has no <pre> block at all (empty/blocked result)
        # but only when we're on a search URL
        if not is_challenge and "lore.kernel.org" in page.url and "?q=" in page.url:
            from bs4 import BeautifulSoup as _BS
            soup = _BS(html, "html.parser")
            if not soup.find("pre"):
                is_challenge = True

        if is_challenge:
            print(f"\n  ⚠️  Challenge/CAPTCHA detected at: {page.url}")
            input("  Solve it in the browser window, then press Enter to continue...\n> ")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    except Exception as e:
        # If we can't even read the page, treat it as a challenge
        print(f"\n  ⚠️  Could not read page content ({e}). Possible challenge.")
        input("  Check the browser window. Press Enter when ready to continue...\n> ")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def search(date_from, date_to, page):
    query = build_query(date_from, date_to)
    print(f"    Query: {query}")

    results  = []
    seen_ids = set()
    offset   = 0

    while len(results) < MAX_THREADS * 3:
        url = build_search_url(query, offset)
        print(f"    Fetching offset {offset}...")

        # Retry loop — complex queries can be slow on the server side
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                wait_if_challenged(page)
                break  # Success — exit retry loop
            except Exception as e:
                print(f"    Timeout/error on attempt {attempt}/{max_retries}: {e}")
                if attempt < max_retries:
                    wait = random.uniform(15, 30)
                    print(f"    Waiting {wait:.1f}s before retry...")
                    time.sleep(wait)
                else:
                    print(f"    All retries exhausted for offset {offset}, skipping chunk.")
                    return results

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        soup = BeautifulSoup(page.content(), "html.parser")

        msg_ids       = extract_msg_ids_from_page(soup)
        found_on_page = 0

        for msg_id in msg_ids:
            if msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)
            date = extract_date_from_msg_id(msg_id)
            results.append((msg_id, date, build_message_url(msg_id)))
            found_on_page += 1

        print(f"    {found_on_page} new results. Total so far: {len(results)}")

        if found_on_page == 0:
            break

        offset += PAGE_STEP
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return results

def download_thread(msg_id, date, subject, output_folder, page):
    download_url = build_download_url(msg_id)
    date_prefix  = f"{date}_" if date else ""
    filename     = f"{date_prefix}{slugify(subject)}.txt"
    filepath     = os.path.join(output_folder, filename)

    if os.path.exists(filepath):
        print(f"    Already exists, skipping: {filename}")
        return filename

    cookies = {c["name"]: c["value"] for c in page.context.cookies()}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        response = requests.get(download_url, cookies=cookies, headers=headers, timeout=30)
        response.raise_for_status()

        try:
            thread_text = gzip.decompress(response.content).decode("utf-8", errors="replace")
        except Exception:
            thread_text = response.content.decode("utf-8", errors="replace")

        thread_text = flag_matching_messages(thread_text)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(thread_text)

        print(f"    Saved: {filename}")
        return filename

    except Exception as e:
        print(f"    ERROR: {e}")
        with open(os.path.join(output_folder, "_failed_downloads.txt"), "a") as log:
            log.write(f"{build_message_url(msg_id)}\n")
        return None

def write_index(downloaded, output_folder, date_from, date_to):
    index_path = os.path.join(output_folder, f"_index_{date_from[:4]}-{date_to[:4]}.txt")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(f"LKML Search Index\n")
        f.write(f"Query    : {QUERY_BASE}\n")
        f.write(f"Date from: {date_from}\n")
        f.write(f"Date to  : {date_to}\n")
        f.write(f"NOTE: Date range is a technical chunk only — not an analytical sub-episode.\n")
        f.write(f"Threads  : {len(downloaded)}\n")
        f.write("=" * 60 + "\n\n")
        for msg_id, date, subject in downloaded:
            f.write(f"{date or 'unknown'} | {subject} | {build_message_url(msg_id)}\n")
    print(f"    Index saved: {os.path.basename(index_path)}")

def run_chunk(date_from, date_to, output_folder, page):
    """Run the compound query for one date chunk."""
    candidates = search(date_from, date_to, page)

    if not candidates:
        print(f"    No results.")
        return 0

    downloaded     = []
    seen_filenames = set()

    for msg_id, date, msg_url in candidates:
        if len(downloaded) >= MAX_THREADS:
            break

        if date_in_range(date, date_from, date_to) is False:
            continue

        subject       = get_subject_from_page(msg_url, page)
        clean_subject = re.sub(r'^(Re:\s*)+', '', subject, flags=re.I).strip()
        filename_key  = slugify(clean_subject)

        if filename_key in seen_filenames:
            print(f"    Duplicate thread, skipping: {clean_subject[:50]}")
            continue

        print(f"    [{len(downloaded)+1}] {date or 'unknown'} | {clean_subject[:55]}")

        filename = download_thread(msg_id, date, clean_subject, output_folder, page)
        if filename:
            seen_filenames.add(filename_key)
            downloaded.append((msg_id, date, subject))

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    if downloaded:
        write_index(downloaded, output_folder, date_from, date_to)

    return len(downloaded)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    output_folder = os.path.join(OUTPUT_BASE, EPISODE, "lkml")
    os.makedirs(output_folder, exist_ok=True)

    print(f"Episode     : {EPISODE}")
    print(f"Output      : {output_folder}")
    print(f"Query       : {QUERY_BASE}")
    print(f"Date chunks : {len(DATE_CHUNKS)} (technical divisions only, not analytical)")
    print(f"Max threads : {MAX_THREADS} per chunk")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page    = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )).new_page()

        print("\nVisiting archive...")
        page.goto(SEARCH_URL, wait_until="domcontentloaded")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        wait_if_challenged(page)

        total = 0

        for date_from, date_to in DATE_CHUNKS:
            print(f"\n{'═'*60}")
            print(f"CHUNK: {date_from} → {date_to}")
            print(f"{'═'*60}")

            n = run_chunk(date_from, date_to, output_folder, page)
            total += n
            print(f"  Chunk done: {n} threads saved.")
            print(f"  Pausing before next chunk...")
            time.sleep(random.uniform(8, 13))

        browser.close()

    print(f"\n{'═'*60}")
    print(f"ALL DONE.")
    print(f"Total threads downloaded: {total}")
    print(f"All files saved to: {output_folder}")
    print(f"{'═'*60}")
    print(f"\nReminder: date chunks are technical divisions only.")
    print(f"Periodization should emerge from the data, not from these boundaries.")

if __name__ == "__main__":
    main()
