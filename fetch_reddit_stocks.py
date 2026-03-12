#!/usr/bin/env python3
"""
Fetch Reddit posts & comments from investment subreddits via Arctic Shift API.
Filters for content recommending, analyzing, or debating specific stocks.
Saves to CSV files capped at 29 MB each.
Supports resuming via a state file.
"""

import csv
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
SUBREDDITS = [
    "SecurityAnalysis",
    "investing",
    "ValueInvesting",
    "stocks",
    "smallcapstocks",
    "TheRaceTo10Million",
]

START_TS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
END_TS   = int(datetime(2026, 3, 11, 23, 59, 59, tzinfo=timezone.utc).timestamp())

BASE_URL  = "https://arctic-shift.photon-reddit.com/api"
LIMIT     = 100
DELAY     = 1.0          # seconds between requests
MAX_FILE_BYTES = 29 * 1024 * 1024

OUTPUT_DIR  = "/home/user/reddit_stock_data"
STATE_FILE  = os.path.join(OUTPUT_DIR, "_state.json")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Stock-mention detection ─────────────────────────────────────────────────────
TICKER_RE = re.compile(
    r'\$[A-Z]{1,6}(?:\.[A-Z]{1,4})?'
    r'|(?<!\w)[A-Z]{2,6}(?:\.[A-Z]{1,4})?(?!\w)',
    re.UNICODE,
)

STOCK_KEYWORDS = re.compile(
    r'\b(?:'
    r'buy|sell|hold|long|short|position|portfolio|shares?|stock|equity|'
    r'valuation|analysis|DD|due diligence|earnings|revenue|EPS|P/?E|'
    r'price target|bull|bear|catalyst|thesis|undervalued|overvalued|'
    r'market cap|dividend|yield|growth|sector|industry|recommend|'
    r'investment|return|upside|downside|risk|fundamental|technical|'
    r'quarter|annual|guidance|outlook|forecast'
    r')\b',
    re.IGNORECASE,
)

NON_TICKERS = {
    "I", "A", "THE", "AND", "OR", "BUT", "FOR", "NOT", "THIS", "WITH",
    "ARE", "WAS", "HAS", "IS", "IT", "AT", "BE", "BY", "DO", "GO",
    "NO", "OF", "ON", "SO", "TO", "UP", "US", "WE", "IF", "IN",
    "MY", "AN", "AS", "HE", "ME", "OK", "AM", "PM", "AMA", "IMO",
    "DD", "ROI", "ETF", "EPS", "PE", "IPO", "SEC", "CEO", "CFO",
    "CTO", "GDP", "USA", "UK", "EU", "IMF", "FED", "AI", "ML",
    "YOY", "QOQ", "TTM", "ATH", "ATL", "FOMO", "YOLO",
    "OP", "OC", "TL", "DR", "TLDR", "FAQ", "AMA", "EDIT", "PS",
    "FYI", "NGL", "TBH", "COVID", "USD", "GBP", "EUR", "JPY",
    "CAD", "AUD", "PDF", "URL", "API", "REIT", "SPAC", "OTC",
}


def is_stock_related(title: str, selftext: str, body: str = "") -> bool:
    combined = f"{title} {selftext} {body}"
    if not STOCK_KEYWORDS.search(combined):
        return False
    tickers = TICKER_RE.findall(combined)
    real_tickers = [
        t.lstrip("$") for t in tickers
        if t.lstrip("$").split(".")[0] not in NON_TICKERS
        and len(t.lstrip("$").split(".")[0]) >= 2
    ]
    return bool(real_tickers)


# ── State persistence ──────────────────────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── CSV writer with auto-rotation at 29 MB ─────────────────────────────────────
class RotatingCSVWriter:
    ALL_FIELDS = [
        "type", "id", "subreddit", "author", "created_utc",
        "title", "selftext", "body", "score", "num_comments",
        "url", "permalink", "link_id", "parent_id",
    ]

    def __init__(self, prefix: str):
        self.prefix = prefix
        # Find the highest existing file number so we append, not overwrite
        self.file_num = 1
        for fn in os.listdir(OUTPUT_DIR):
            if fn.startswith(prefix) and fn.endswith(".csv") and not fn.startswith("_"):
                try:
                    num = int(fn.replace(prefix + "_", "").replace(".csv", ""))
                    self.file_num = max(self.file_num, num)
                except ValueError:
                    pass
        self._fh = None
        self._writer = None
        self._current_path = None
        # Re-open the last file if it's under the size limit, else start a new one
        candidate = os.path.join(OUTPUT_DIR, f"{self.prefix}_{self.file_num:03d}.csv")
        if os.path.exists(candidate) and os.path.getsize(candidate) < MAX_FILE_BYTES:
            self._current_path = candidate
            self._fh = open(self._current_path, "a", newline="", encoding="utf-8")
            self._writer = csv.DictWriter(
                self._fh, fieldnames=self.ALL_FIELDS, extrasaction="ignore"
            )
            print(f"  [CSV] Appending to {self._current_path} "
                  f"({os.path.getsize(candidate)/1024/1024:.2f} MB)")
        else:
            if os.path.exists(candidate):
                self.file_num += 1
            self._open_new()

    def _open_new(self):
        if self._fh:
            self._fh.close()
        self._current_path = os.path.join(
            OUTPUT_DIR, f"{self.prefix}_{self.file_num:03d}.csv"
        )
        self._fh = open(self._current_path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._fh, fieldnames=self.ALL_FIELDS, extrasaction="ignore"
        )
        self._writer.writeheader()
        self._fh.flush()
        print(f"  [CSV] Opened {self._current_path}")

    def write(self, row: dict):
        self._writer.writerow(row)
        self._fh.flush()
        if os.path.getsize(self._current_path) >= MAX_FILE_BYTES:
            self.file_num += 1
            self._open_new()

    def close(self):
        if self._fh:
            self._fh.close()


# ── HTTP helper ────────────────────────────────────────────────────────────────
def api_get(endpoint: str, params: dict, retries: int = 5) -> dict:
    url = f"{BASE_URL}/{endpoint}?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "stock-research-fetcher/1.0 (educational use)",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            if e.code == 429:
                wait = 10 * (attempt + 1)
                print(f"    Rate limited – sleeping {wait}s …")
                time.sleep(wait)
            elif e.code == 422:
                print(f"    422 Unprocessable: {body[:200]} | params={params}")
                # Unrecoverable for this window – signal caller to stop
                raise ValueError(f"422 from API: {body[:200]}")
            else:
                raise RuntimeError(f"HTTP {e.code}: {body[:200]}")
        except ValueError:
            raise
        except Exception as e:
            wait = 2 ** attempt
            print(f"    Request error ({e}) – retry in {wait}s …")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ── Generic paginated fetcher ──────────────────────────────────────────────────
def fetch_items(
    endpoint: str,
    label: str,
    subreddit: str,
    start_after: int,
    writer: RotatingCSVWriter,
    state: dict,
    state_key: str,
    row_builder,
) -> int:
    after = start_after
    total = 0
    batch = 0

    while after < END_TS:
        params = {
            "subreddit": subreddit,
            "after":  after,
            "before": END_TS,
            "limit":  LIMIT,
            "sort":   "asc",
        }
        try:
            data = api_get(endpoint, params)
        except ValueError:
            # 422 – window is invalid, stop this segment
            break

        items = data.get("data", [])
        if not items:
            break

        batch += 1
        kept = 0
        for item in items:
            row = row_builder(item, subreddit)
            if row:
                writer.write(row)
                kept += 1
                total += 1

        last_ts = items[-1].get("created_utc") or items[-1].get("created", 0)
        print(
            f"  [{label}/{subreddit}] batch {batch:4d} | fetched {len(items):3d} "
            f"| kept {kept:3d} | up to {datetime.fromtimestamp(last_ts, tz=timezone.utc).date()}"
        )

        # Save progress after every batch
        state[state_key] = last_ts + 1
        save_state(state)

        if len(items) < LIMIT:
            break

        # Advance by +1 to avoid re-fetching items at the same timestamp
        after = last_ts + 1
        time.sleep(DELAY)

    # Mark as fully done
    state[state_key] = END_TS + 1
    save_state(state)
    return total


# ── Row builders ───────────────────────────────────────────────────────────────
def build_post_row(post: dict, subreddit: str):
    title    = post.get("title", "") or ""
    selftext = post.get("selftext", "") or ""
    if selftext in ("[removed]", "[deleted]"):
        selftext = ""
    if not is_stock_related(title, selftext):
        return None
    created = post.get("created_utc") or post.get("created", 0)
    return {
        "type":         "post",
        "id":           post.get("id"),
        "subreddit":    subreddit,
        "author":       post.get("author"),
        "created_utc":  datetime.fromtimestamp(created, tz=timezone.utc).isoformat(),
        "title":        title,
        "selftext":     selftext[:10000],
        "score":        post.get("score"),
        "num_comments": post.get("num_comments"),
        "url":          post.get("url"),
        "permalink":    post.get("permalink"),
    }


def build_comment_row(comment: dict, subreddit: str):
    body = comment.get("body", "") or ""
    if body in ("[removed]", "[deleted]"):
        body = ""
    if not is_stock_related("", "", body):
        return None
    created = comment.get("created_utc") or comment.get("created", 0)
    return {
        "type":        "comment",
        "id":          comment.get("id"),
        "subreddit":   subreddit,
        "author":      comment.get("author"),
        "created_utc": datetime.fromtimestamp(created, tz=timezone.utc).isoformat(),
        "link_id":     comment.get("link_id"),
        "parent_id":   comment.get("parent_id"),
        "body":        body[:5000],
        "score":       comment.get("score"),
    }


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"Date range : {datetime.fromtimestamp(START_TS, tz=timezone.utc).date()} → "
          f"{datetime.fromtimestamp(END_TS, tz=timezone.utc).date()}")
    print(f"Output dir : {OUTPUT_DIR}\n")

    state  = load_state()
    writer = RotatingCSVWriter("reddit_stocks")

    grand_posts    = 0
    grand_comments = 0

    for sub in SUBREDDITS:
        print(f"\n{'='*60}")
        print(f"Subreddit: r/{sub}")
        print(f"{'='*60}")

        # ── Posts ──
        posts_key    = f"{sub}:posts:after"
        posts_done_key = f"{sub}:posts:done"
        if state.get(posts_done_key) or state.get(posts_key, 0) > END_TS:
            print("  Posts: already complete (skipping)")
        else:
            posts_after = state.get(posts_key, START_TS)
            if posts_after > START_TS:
                print(f"  Posts: resuming from {datetime.fromtimestamp(posts_after, tz=timezone.utc).isoformat()}")
            else:
                print("  Fetching posts …")
            n = fetch_items(
                "posts/search", "posts", sub, posts_after,
                writer, state, posts_key, build_post_row,
            )
            state[posts_done_key] = True
            save_state(state)
            print(f"  → {n} posts saved")
            grand_posts += n

        # ── Comments ──
        cmts_key      = f"{sub}:comments:after"
        cmts_done_key = f"{sub}:comments:done"
        if state.get(cmts_done_key) or state.get(cmts_key, 0) > END_TS:
            print("  Comments: already complete (skipping)")
        else:
            cmts_after = state.get(cmts_key, START_TS)
            if cmts_after > START_TS:
                print(f"  Comments: resuming from {datetime.fromtimestamp(cmts_after, tz=timezone.utc).isoformat()}")
            else:
                print("  Fetching comments …")
            n = fetch_items(
                "comments/search", "cmts", sub, cmts_after,
                writer, state, cmts_key, build_comment_row,
            )
            state[cmts_done_key] = True
            save_state(state)
            print(f"  → {n} comments saved")
            grand_comments += n

    writer.close()

    print(f"\n{'='*60}")
    print(f"Done.")
    print(f"  Total posts    : {grand_posts:,}")
    print(f"  Total comments : {grand_comments:,}")
    print(f"  CSV files:")
    for fn in sorted(os.listdir(OUTPUT_DIR)):
        if fn.endswith(".csv") and not fn.startswith("_"):
            size = os.path.getsize(os.path.join(OUTPUT_DIR, fn))
            print(f"    {fn}  ({size/1024/1024:.2f} MB)")


if __name__ == "__main__":
    main()
