name: Jugantor Scraper — Daily 3 AM BST

# ─────────────────────────────────────────────────────────────────────────────
#  Bangladesh Standard Time (BST) = UTC+6
#  3:00 AM BST  =  21:00 (9 PM) UTC previous day
#  cron syntax  :  minute  hour  dom  month  dow
# ─────────────────────────────────────────────────────────────────────────────
on:
  schedule:
    - cron: "0 21 * * *"      # 21:00 UTC = 03:00 AM BST next day

  # Allow manual trigger from GitHub Actions tab
  workflow_dispatch:
    inputs:
      reset:
        description: "Reset DB and start from page 0? (yes/no)"
        required: false
        default: "no"

# Only one run at a time — cancel any in-progress run if re-triggered
concurrency:
  group: jugantor-scraper
  cancel-in-progress: false

jobs:
  scrape:
    name: Run Jugantor Dual-Stream Scraper
    runs-on: ubuntu-latest
    timeout-minutes: 300     # 5-hour hard cap (GitHub max is 6h)

    permissions:
      contents: write        # needed to push jugantor.db back to repo

    steps:
      # ── 1. Checkout repo ───────────────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 1
          token: ${{ secrets.GITHUB_TOKEN }}

      # ── 2. Python setup ────────────────────────────────────────────────────
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      # ── 3. Install dependencies ────────────────────────────────────────────
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install requests aiohttp tqdm colorama

      # ── 4. Restore DB from cache (so we keep history across runs) ──────────
      - name: Restore database cache
        uses: actions/cache@v4
        with:
          path: jugantor.db
          key: jugantor-db-${{ github.run_number }}
          restore-keys: |
            jugantor-db-

      # ── 5. Show DB status before run ───────────────────────────────────────
      - name: Pre-run database info
        run: |
          python - << 'EOF'
          import sqlite3, os
          db = "jugantor.db"
          if os.path.exists(db):
              conn = sqlite3.connect(db)
              total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
              even  = conn.execute("SELECT value FROM scrape_state WHERE key='cp_even'").fetchone()
              odd   = conn.execute("SELECT value FROM scrape_state WHERE key='cp_odd'").fetchone()
              conn.close()
              print(f"✅ DB found: {total:,} articles stored")
              print(f"   EVEN checkpoint : {even[0] if even else 'none (fresh)'}")
              print(f"   ODD  checkpoint : {odd[0]  if odd  else 'none (fresh)'}")
          else:
              print("ℹ️  No existing DB — fresh start")
          EOF

      # ── 6. Handle optional reset (manual trigger only) ────────────────────
      - name: Apply reset if requested
        if: github.event_name == 'workflow_dispatch' && github.event.inputs.reset == 'yes'
        run: |
          echo "⚠️  RESET requested — removing existing DB"
          rm -f jugantor.db
          echo "RESET=true" >> $GITHUB_ENV

      # ── 7. Run the scraper ─────────────────────────────────────────────────
      - name: Run scraper
        run: |
          echo "🕒 Starting scraper at $(TZ='Asia/Dhaka' date '+%Y-%m-%d %H:%M:%S BST')"
          python scraper.py
          echo "✅ Scraper finished at $(TZ='Asia/Dhaka' date '+%Y-%m-%d %H:%M:%S BST')"

      # ── 8. Post-run summary ────────────────────────────────────────────────
      - name: Post-run database summary
        if: always()
        run: |
          python - << 'EOF'
          import sqlite3, os
          db = "jugantor.db"
          if not os.path.exists(db):
              print("❌ Database file not found"); exit(0)
          conn = sqlite3.connect(db)

          total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
          even  = conn.execute("SELECT value FROM scrape_state WHERE key='cp_even'").fetchone()
          odd   = conn.execute("SELECT value FROM scrape_state WHERE key='cp_odd'").fetchone()

          # Last run stats
          run = conn.execute(
              "SELECT * FROM scrape_runs ORDER BY run_id DESC LIMIT 1"
          ).fetchone()

          conn.close()

          print("=" * 52)
          print("  📊  SCRAPE SUMMARY")
          print("=" * 52)
          print(f"  Total articles in DB  : {total:,}")
          if run:
              cols = ["run_id","started_at","ended_at","mode",
                      "min_page","max_page","workers","new_saved","duplicates","errors"]
              r = dict(zip(cols, run))
              print(f"  This run — new saved  : {r.get('new_saved',0):,}")
              print(f"  This run — duplicates : {r.get('duplicates',0):,}")
              print(f"  This run — errors     : {r.get('errors',0)}")
              print(f"  Mode                  : {r.get('mode','?')}")
              print(f"  Started               : {r.get('started_at','?')}")
              print(f"  Ended                 : {r.get('ended_at','?')}")
          print(f"  EVEN checkpoint       : {even[0] if even else '?'}")
          print(f"  ODD  checkpoint       : {odd[0]  if odd  else '?'}")
          print("=" * 52)
          EOF

      # ── 9. Commit DB back to repo ─────────────────────────────────────────
      - name: Commit updated database
        if: always()
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # Only commit if DB actually changed
          if git diff --quiet jugantor.db 2>/dev/null || \
             ! git ls-files --error-unmatch jugantor.db 2>/dev/null; then
            git add jugantor.db scraper.log 2>/dev/null || true
            TIMESTAMP=$(TZ='Asia/Dhaka' date '+%Y-%m-%d %H:%M BST')
            git commit -m "🤖 scraper run — ${TIMESTAMP}" \
              --allow-empty \
              -m "Automated daily scrape (3 AM BST)" || echo "Nothing new to commit"
            git push
          else
            echo "ℹ️  No changes to commit"
          fi

      # ── 10. Upload DB as artifact (30-day retention) ───────────────────────
      - name: Upload database artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: jugantor-db-${{ github.run_number }}
          path: |
            jugantor.db
            jugantor_scraper.log
          retention-days: 30
          if-no-files-found: ignore
