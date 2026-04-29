# Jugantor Dual-Stream Scraper

Automated news scraper for [jugantor.com](https://www.jugantor.com) — runs daily at **3:00 AM Bangladesh Standard Time (BST)** via GitHub Actions.

## How it works

| Stream | Pages scraped |
|--------|---------------|
| EVEN   | 0, 2, 4, 6, … |
| ODD    | 1, 3, 5, 7, … |

Both streams run concurrently and share a single SQLite database (`jugantor.db`).  
**Duplicate URLs are impossible** — enforced at the database level with a `UNIQUE(url)` constraint and `INSERT OR IGNORE`.

## Schedule

```
Cron : 0 21 * * *   (UTC)
Time : 3:00 AM BST  (UTC+6)
```

## Configuration

All settings live at the top of `scraper.py` inside the `CONFIG` dict:

```python
CONFIG = {
    "MODE":     "threadpool",   # normal | threadpool | asyncio
    "MIN_PAGE": 0,
    "MAX_PAGE": 1000,
    "WORKERS":  6,
    "DB_FILE":  "jugantor.db",
    ...
}
```

Edit `CONFIG` and push — the next scheduled run picks up the new settings automatically.

## Manual trigger

1. Go to **Actions** tab in your repo
2. Click **Jugantor Scraper — Daily 3 AM BST**
3. Click **Run workflow**
4. Optionally set **reset = yes** to wipe the DB and start fresh

## Database schema

```sql
articles (
    id             INTEGER PRIMARY KEY,
    article_id     INTEGER,
    title          TEXT,
    url            TEXT UNIQUE,   -- ← duplicate prevention
    published_date TEXT,
    stream         TEXT,          -- 'even' or 'odd'
    page_number    INTEGER,
    scraped_at     TEXT
)

scrape_state  -- per-stream resume checkpoints
scrape_runs   -- audit log of every execution
```

## Auto-resume

Each run saves a checkpoint after every page.  
If a run is cancelled or fails mid-way, the next run continues exactly from where it stopped — no pages are re-scraped, no duplicates are inserted.

## Artifacts

Every run uploads `jugantor.db` as a downloadable GitHub artifact (30-day retention).

## Local run

```bash
pip install -r requirements.txt
python scraper.py
```

## Dependencies

```
requests   — HTTP client (normal + threadpool modes)
aiohttp    — async HTTP client (asyncio mode)
tqdm       — progress bar
colorama   — coloured terminal output
```
