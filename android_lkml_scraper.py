import os
import re
import time
import gzip
import random
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — the only section you need to edit between runs
# ══════════════════════════════════════════════════════════════════════════════

# --- EPISODE AND OUTPUT SETTINGS ---
EPISODE     = "android_episode"   # One folder for the entire episode
OUTPUT_BASE = r"C:\Users\pedra\Downloads\lkmlsample"

# --- DATE CHUNKS ---
# The full episode spans 2007-2013. We break it into two-year windows
# purely for technical reasons (server load, manageable run sizes).
# These boundaries carry NO analytical meaning — do not interpret them
# as sub-episodes. Periodization will emerge from the data.
DATE_CHUNKS = [
    ("2007-01-01", "2008-12-31"),
    ("2009-01-01", "2010-12-31"),
    ("2011-01-01", "2012-12-31"),
    ("2013-01-01", "2013-12-31"),
]

# --- KEYWORD SET A  (transparency / disclosure / access norms) ---
# A thread must match ≥1 keyword from SET A  AND  ≥1 keyword from SET B.
# Format: (keyword, use_exact_phrase)
#
# use_exact_phrase=True  → wraps in quotes → exact phrase match
# use_exact_phrase=False → no quotes → both words must appear anywhere
#
KEYWORDS_SET_A = [
    ("NDA",                        False),
    ("confidentiality disclaimer", True),
    ("confidentiality",            False),
    ("proprietary",                False),
    ("closed-source",              False),
    ("closed source",              True),
    ("off-list",                   False),
    ("private discussion",         True),
    ("private thread",             True),
    ("private email",              True),
    ("on-list",                    False),
    ("public discussion",          True),
    ("public review",              True),
    ("transparency",               False),
    ("lack of transparency",       True),
    ("behind the scenes",          True),
    ("embargo",                    False),
    ("full disclosure",            True),
    ("responsible disclosure",     True),
    ("binary blob",                True),
    ("binary blobs",               True),
    ("secret",                     False),
    ("secretly",                   False),
    ("hidden",                     False),
]

# --- KEYWORD SET B  (Android-specific technical and institutional terms) ---
KEYWORDS_SET_B = [
    ("wakelock",              False),  # Central technical flashpoint
    ("android staging",       False),  # Staging tree — key institutional mechanism
    ("android upstream",      False),  # Core transparency debate framing
    ("android mainline",      False),  # Reintegration framing
    ("google android",        False),  # Corporate vs. community framing
    ("code dump",             True),   # Exact phrase — community's label for Google's approach
    ("open development",      False),  # Direct transparency norm articulations
    ("kroah-hartman android", False),  # Actor-specific — GKH is central to this episode
]

# --- REQUEST TIMING ---
DELAY_MIN  = 3    # Minimum seconds between requests
DELAY_MAX  = 7    # Maximum seconds between requests (randomised to avoid detection)

# --- BATCH CONFIRMATION ---
BATCH_SIZE = 100  # After every N downloads, pause and ask whether to continue

# ══════════════════════════════════════════════════════════════════════════════
# SOURCE SETTINGS — edit this section when adapting for a new data source
# ══════════════════════════════════════════════════════════════════════════════

SEARCH_URL = "https://lore.kernel.org/lkml/"
PAGE_STEP  = 200

# ══════════════════════════════════════════════════════════════════════════════
# ANTI-BAN HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def random_sleep(lo=None, hi=None):
    """Sleep a random duration between lo and hi seconds."""
    lo = lo if lo is not None else DELAY_MIN
    hi = hi if hi is not None else DELAY_MAX
    time.sleep(random.uniform(lo, hi))


def check_for_block(page):
    """
    Detect captcha / rate-limit / access-denied responses.
    If a block signal is found, pause and ask the user to resolve it
    in the open browser window, then press Enter to resume.
    """
    try:
        content = page.content().lower()
        title   = (page.title() or "").lower()
    except Exception:
        return

    signals = [
        "captcha", "too many requests", "rate limit", "rate-limit",
        "access denied", "403 forbidden", "429 too many",
        "blocked", "please verify", "unusual traffic",
    ]

    if any(s in content or s in title for s in signals):
        print("\n" + "!" * 60)
        print("  BLOCK / CAPTCHA DETECTED.")
        print("  Please resolve the issue in the open browser window.")
        print("!" * 60)
        input("  Press Enter here once the page loads normally... ")

# ══════════════════════════════════════════════════════════════════════════════
# URL / QUERY BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def build_query(keyword, use_exact_phrase, date_from, date_to):
    d_from = date_from.replace("-", "")
    d_to   = date_to.replace("-", "")
    if use_exact_phrase:
        return f'"{keyword}" d:{d_from}..{d_to}'
    return f'{keyword} d:{d_from}..{d_to}'


def build_search_url(query, offset):
    return f"{SEARCH_URL}?q={requests.utils.quote(query)}&o={offset}"


def build_message_url(msg_id):
    return f"{SEARCH_URL}{msg_id}/"


def build_download_url(msg_id):
    return f"{SEARCH_URL}{msg_id}/t.mbox.gz"

# ══════════════════════════════════════════════════════════════════════════════
# PAGE PARSERS
# ══════════════════════════════════════════════════════════════════════════════

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


def extract_date_from_msg_id(msg_id):
    match = re.match(r'^(20\d{2})(\d{2})(\d{2})\d+', msg_id)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None

# ══════════════════════════════════════════════════════════════════════════════
# CORE HELPERS
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
        check_for_block(page)
        random_sleep(0.5, 1.5)
        soup      = BeautifulSoup(page.content(), "html.parser")
        title_tag = soup.find("title")
        subject   = title_tag.get_text(strip=True) if title_tag else "untitled"
        subject   = re.sub(r'\s*-\s*lore\.kernel\.org.*$', '', subject, flags=re.I)
        return subject.strip()
    except Exception as e:
        print(f"    WARNING: could not get subject: {e}")
        return "untitled"


def flag_matching_messages(thread_text, matched_keywords):
    """
    Add an X-Dissertation-Match header to every message that contains
    at least one of the matched keywords (from either keyword set).
    """
    messages = thread_text.split("\nFrom ")
    flagged  = []
    for message in messages:
        hits = [kw for kw in matched_keywords if kw.lower() in message.lower()]
        if hits:
            flag_line = f"X-Dissertation-Match: KEYWORD_MATCH ({', '.join(hits)})\n"
            lines     = message.split("\n", 1)
            message   = lines[0] + "\n" + flag_line + (lines[1] if len(lines) == 2 else "")
        flagged.append(message)
    return "\nFrom ".join(flagged)

# ══════════════════════════════════════════════════════════════════════════════
# SEARCH  —  collect ALL message IDs for a keyword / keyword set
# ══════════════════════════════════════════════════════════════════════════════

def search_all_ids(keyword, use_exact_phrase, date_from, date_to, page):
    """
    Paginate through every result page for one keyword + date range.
    Returns {msg_id: date_str_or_None} — no cap on result count.
    """
    query = build_query(keyword, use_exact_phrase, date_from, date_to)
    print(f"        Query: {query}")

    results  = {}
    seen_ids = set()
    offset   = 0

    while True:
        url = build_search_url(query, offset)
        print(f"        offset={offset}...", end=" ", flush=True)

        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        check_for_block(page)
        random_sleep()

        soup          = BeautifulSoup(page.content(), "html.parser")
        msg_ids       = extract_msg_ids_from_page(soup)
        found_on_page = 0

        for msg_id in msg_ids:
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                results[msg_id] = extract_date_from_msg_id(msg_id)
                found_on_page += 1

        print(f"{found_on_page} new  (keyword total: {len(results)})")

        if found_on_page == 0:
            break

        offset += PAGE_STEP
        random_sleep()

    return results


def collect_set_ids(keywords_list, date_from, date_to, page, set_label):
    """
    Union of all msg_ids matching ANY keyword in keywords_list.
    Returns {msg_id: date_str_or_None}.
    """
    all_ids = {}
    for i, (keyword, use_exact_phrase) in enumerate(keywords_list, 1):
        print(f"      [{set_label}  {i}/{len(keywords_list)}] '{keyword}'")
        ids = search_all_ids(keyword, use_exact_phrase, date_from, date_to, page)
        new = {k: v for k, v in ids.items() if k not in all_ids}
        all_ids.update(new)
        print(f"      {set_label} running union: {len(all_ids)} unique IDs  (+{len(new)} new)")
        if i < len(keywords_list):
            random_sleep(DELAY_MAX, DELAY_MAX * 2)   # longer pause between keywords
    return all_ids

# ══════════════════════════════════════════════════════════════════════════════
# DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def download_thread(msg_id, date, subject, output_folder, all_keywords, page):
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

        thread_text = flag_matching_messages(thread_text, all_keywords)

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
    index_path = os.path.join(
        output_folder,
        f"_index_intersection_{date_from[:4]}-{date_to[:4]}.txt"
    )
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("LKML Search Index — Intersection of Set A ∩ Set B\n")
        f.write(f"Date range : {date_from} → {date_to}\n")
        f.write(f"Set A      : {len(KEYWORDS_SET_A)} keywords (transparency / disclosure)\n")
        f.write(f"Set B      : {len(KEYWORDS_SET_B)} keywords (Android / technical)\n")
        f.write(f"Threads    : {len(downloaded)}\n")
        f.write("NOTE: Date range is a technical chunk — not an analytical sub-episode.\n")
        f.write("=" * 60 + "\n\n")
        for msg_id, date, subject in downloaded:
            f.write(f"{date or 'unknown'} | {subject} | {build_message_url(msg_id)}\n")
    print(f"    Index saved: {os.path.basename(index_path)}")

# ══════════════════════════════════════════════════════════════════════════════
# CHUNK RUNNER  —  Set A ∩ Set B for one date window
# ══════════════════════════════════════════════════════════════════════════════

def run_chunk(date_from, date_to, output_folder, page):
    """Collect Set A ∩ Set B for one date chunk, then download ALL results."""

    print(f"\n  {'─' * 55}")
    print(f"  Collecting Set A  (transparency / disclosure keywords)...")
    print(f"  {'─' * 55}")
    set_a_ids = collect_set_ids(KEYWORDS_SET_A, date_from, date_to, page, "SET-A")

    print(f"\n  {'─' * 55}")
    print(f"  Collecting Set B  (Android / technical keywords)...")
    print(f"  {'─' * 55}")
    set_b_ids = collect_set_ids(KEYWORDS_SET_B, date_from, date_to, page, "SET-B")

    intersection = {mid: date for mid, date in set_a_ids.items() if mid in set_b_ids}

    print(f"\n  ┌{'─' * 50}┐")
    print(f"  │  Set A  (transparency):  {len(set_a_ids):>6} unique threads          │")
    print(f"  │  Set B  (Android):       {len(set_b_ids):>6} unique threads          │")
    print(f"  │  Intersection A ∩ B:     {len(intersection):>6} threads  ← to download │")
    print(f"  └{'─' * 50}┘")

    if not intersection:
        print("  No threads in intersection. Skipping to next chunk.")
        return 0

    answer = input(
        f"\n  Download all {len(intersection)} threads for {date_from} → {date_to}? [y/n]: "
    ).strip().lower()
    if answer != 'y':
        print("  Skipped.")
        return 0

    all_keywords   = [kw for kw, _ in KEYWORDS_SET_A + KEYWORDS_SET_B]
    downloaded     = []
    seen_filenames = set()
    to_download    = list(intersection.items())
    batch_paused_at = -1   # tracks the download count at which we last paused

    for msg_id, date in to_download:
        n_downloaded = len(downloaded)

        # Pause every BATCH_SIZE completed downloads
        if (n_downloaded > 0
                and n_downloaded % BATCH_SIZE == 0
                and n_downloaded != batch_paused_at):
            batch_paused_at = n_downloaded
            remaining = len(to_download) - to_download.index((msg_id, date))
            print(f"\n  ── {n_downloaded} threads downloaded so far ──")
            answer = input(
                f"  Continue downloading the remaining ~{remaining} threads? [y/n]: "
            ).strip().lower()
            if answer != 'y':
                print("  Stopping early.")
                break

        if date_in_range(date, date_from, date_to) is False:
            continue

        msg_url       = build_message_url(msg_id)
        subject       = get_subject_from_page(msg_url, page)
        clean_subject = re.sub(r'^(Re:\s*)+', '', subject, flags=re.I).strip()
        filename_key  = slugify(clean_subject)

        if filename_key in seen_filenames:
            print(f"    Duplicate thread, skipping: {clean_subject[:50]}")
            continue

        print(f"    [{len(downloaded)+1}/{len(intersection)}] {date or 'unknown'} | {clean_subject[:55]}")

        filename = download_thread(msg_id, date, clean_subject, output_folder, all_keywords, page)
        if filename:
            seen_filenames.add(filename_key)
            downloaded.append((msg_id, date, subject))

        random_sleep()

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
    print(f"Set A       : {len(KEYWORDS_SET_A)} keywords  (transparency / disclosure)")
    print(f"Set B       : {len(KEYWORDS_SET_B)} keywords  (Android / technical)")
    print(f"Date chunks : {len(DATE_CHUNKS)}  (technical divisions — not analytical sub-episodes)")
    print(f"Batch size  : pause and confirm after every {BATCH_SIZE} downloads")
    print(f"Delay       : {DELAY_MIN}–{DELAY_MAX} s  (randomised per request)")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ))
        page = context.new_page()

        print("\nVisiting archive homepage...")
        page.goto(SEARCH_URL, wait_until="domcontentloaded")
        check_for_block(page)
        random_sleep()

        total = 0

        for date_from, date_to in DATE_CHUNKS:
            print(f"\n{'═' * 60}")
            print(f"DATE CHUNK: {date_from}  →  {date_to}")
            print(f"{'═' * 60}")

            n = run_chunk(date_from, date_to, output_folder, page)
            total += n
            print(f"\n  Chunk done: {n} threads saved.")

            if (date_from, date_to) != DATE_CHUNKS[-1]:
                pause = random.uniform(DELAY_MAX * 2, DELAY_MAX * 3)
                print(f"  Pausing {pause:.1f}s before next chunk...")
                time.sleep(pause)

        browser.close()

    print(f"\n{'═' * 60}")
    print(f"ALL DONE.")
    print(f"Total threads downloaded : {total}")
    print(f"Files saved to           : {output_folder}")
    print(f"{'═' * 60}")
    print(f"\nReminder: date chunks are technical divisions only.")
    print(f"Periodization should emerge from the data, not from these boundaries.")


if __name__ == "__main__":
    main()
