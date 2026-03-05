"""
Fetch r/ValueInvesting posts & comments (Nov 1 2025 – Mar 4 2026)
via the Arctic Shift API, filter for stock discussion, and save to CSV.
"""

import csv
import re
import time
import sys
from datetime import datetime, timezone

import requests

# ── Config ────────────────────────────────────────────────────────────────────
SUBREDDIT   = "ValueInvesting"
DATE_START  = datetime(2025, 11, 1,  tzinfo=timezone.utc)
DATE_END    = datetime(2026,  3,  4, 23, 59, 59, tzinfo=timezone.utc)
TS_START    = int(DATE_START.timestamp())
TS_END      = int(DATE_END.timestamp())

BASE_URL    = "https://arctic-shift.photon-reddit.com/api"
LIMIT       = 100          # max per page
DELAY       = 1.0          # seconds between requests
OUTPUT_CSV  = "valueinvesting_stocks.csv"

# ── Stock-mention detection ───────────────────────────────────────────────────
# Matches $TICK, plain all-caps 1-5 letter tickers, or stock-related keywords
TICKER_RE   = re.compile(r'\$[A-Z]{1,5}|(?<!\w)[A-Z]{1,5}(?!\w)')
STOCK_KEYWORDS = re.compile(
    r'\b(stock|ticker|shares?|equity|valuation|undervalued|overvalued|'
    r'P/E|EPS|earnings|revenue|margin|dividend|buyback|short|long|position|'
    r'buy|sell|hold|bull|bear|DCF|moat|intrinsic value|market cap|'
    r'price target|analyst|recommendation|portfolio|invest)\b',
    re.IGNORECASE,
)

# Common words that look like tickers but aren't – used as a rough filter
NON_TICKERS = {
    "A","I","AM","AN","AS","AT","BE","BY","DO","GO","HE","IF","IN","IS","IT",
    "ME","MY","NO","OF","OK","ON","OR","SO","TO","UP","US","WE","THE","AND",
    "FOR","ARE","BUT","NOT","YOU","ALL","CAN","HER","WAS","ONE","OUR","OUT",
    "HAD","HIS","HOW","ITS","WHO","DID","GET","HAS","HIM","LET","MAY","PUT",
    "SAY","SHE","TOO","USE","WAY","WHO","WHY","GOT","OLD","SEE","TWO","YET",
    "EDIT","TLDR","IMO","ELI","LOL","OMG","AMA","FAQ","GFY","CEO","CFO","COO",
    "IPO","ETF","SEC","IRS","GDP","USA","USD","EUR","GBP","CPI","PPI","YOY",
    "QOQ","TTM","LTM","NTM","FY","Q1","Q2","Q3","Q4","YE","VC","PE","OP",
}

def looks_like_stock_post(text: str) -> bool:
    """Return True when the text appears to discuss a specific stock."""
    if not text:
        return False
    if STOCK_KEYWORDS.search(text):
        return True
    tickers = [m for m in TICKER_RE.findall(text) if m.lstrip("$") not in NON_TICKERS]
    return len(tickers) >= 2   # at least 2 distinct ticker-like tokens


def ts_to_iso(ts) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


# ── API helpers ───────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ValueInvestingResearch/1.0"})

def api_get(endpoint: str, params: dict) -> dict:
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(1, 6):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  [warn] {exc} – retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after 5 attempts")


def fetch_all(endpoint: str, extra_params: dict) -> list[dict]:
    """Page through an Arctic Shift search endpoint and return all items."""
    items   = []
    after   = TS_START
    page    = 0

    while True:
        params = {
            "subreddit": SUBREDDIT,
            "after":     after,
            "before":    TS_END,
            "limit":     LIMIT,
            "sort":      "asc",
            **extra_params,
        }
        page += 1
        data = api_get(endpoint, params)

        # Arctic Shift wraps results in {"data": [...]}
        batch = data.get("data", [])
        if not batch:
            print(f"  [{endpoint}] page {page}: 0 items – done.")
            break

        items.extend(batch)
        print(f"  [{endpoint}] page {page}: {len(batch)} items "
              f"(total so far: {len(items)})")

        if len(batch) < LIMIT:
            break                       # last page

        # Advance cursor: use created_utc of last item + 1
        after = int(batch[-1]["created_utc"]) + 1
        if after > TS_END:
            break

        time.sleep(DELAY)

    return items


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Fetching r/{SUBREDDIT}  {DATE_START.date()} → {DATE_END.date()}")

    # ── Posts ─────────────────────────────────────────────────────────────────
    print("\n=== Posts ===")
    raw_posts = fetch_all("posts/search", {})

    stock_posts = []
    for p in raw_posts:
        title    = p.get("title", "")
        selftext = p.get("selftext", "")
        combined = f"{title} {selftext}"
        if looks_like_stock_post(combined):
            stock_posts.append({
                "type":          "post",
                "id":            p.get("id"),
                "created_utc":   ts_to_iso(p.get("created_utc", 0)),
                "author":        p.get("author", ""),
                "title":         title,
                "selftext":      selftext,
                "score":         p.get("score", 0),
                "num_comments":  p.get("num_comments", 0),
                "url":           p.get("url", ""),
                "permalink":     "https://reddit.com" + p.get("permalink", ""),
                "link_flair_text": p.get("link_flair_text", ""),
                "body":          "",           # N/A for posts
            })

    print(f"Posts fetched: {len(raw_posts)} | Stock-related: {len(stock_posts)}")

    # ── Comments ──────────────────────────────────────────────────────────────
    print("\n=== Comments ===")
    raw_comments = fetch_all("comments/search", {})

    stock_comments = []
    for c in raw_comments:
        body = c.get("body", "")
        if looks_like_stock_post(body):
            stock_comments.append({
                "type":          "comment",
                "id":            c.get("id"),
                "created_utc":   ts_to_iso(c.get("created_utc", 0)),
                "author":        c.get("author", ""),
                "title":         "",           # N/A for comments
                "selftext":      "",           # N/A for comments
                "score":         c.get("score", 0),
                "num_comments":  "",
                "url":           "",
                "permalink":     "https://reddit.com" + c.get("permalink", ""),
                "link_flair_text": "",
                "body":          body,
            })

    print(f"Comments fetched: {len(raw_comments)} | Stock-related: {len(stock_comments)}")

    # ── Write CSV ─────────────────────────────────────────────────────────────
    all_rows = sorted(
        stock_posts + stock_comments,
        key=lambda r: r["created_utc"],
    )

    fieldnames = [
        "type", "id", "created_utc", "author", "score",
        "num_comments", "title", "selftext", "body",
        "link_flair_text", "url", "permalink",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nSaved {len(all_rows)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
