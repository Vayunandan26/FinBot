# Data Setup Guide for Teammates
## Business & Finance RAG Chatbot — Pipeline Data Access

This project uses:
- **Git** for code version control
- **DVC** for tracking which version of the data exists (pointer files only)
- **rclone + Google Drive** for the actual data sharing

You do not need a GCP account. You only need a regular Gmail account and
access to the shared Google Drive folder (the pipeline owner will share it with you).

---

## How Data Flows

```
Owner's machine                  Google Drive              Your machine
────────────────                 ────────────              ────────────
batch_pipeline.py runs
  → cleans a batch
  → sync_to_shared()   ──────→  cleaned/ folder  ──────→  teammate_sync.py
  → next batch...                 grows live                pulls every 60s
```

---

## One-Time Setup (do this once)

### 1. Clone the repo
```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Install DVC
```bash
pip install dvc
```

### 4. Install rclone

**Mac:**
```bash
brew install rclone
```

**Linux:**
```bash
sudo apt install rclone
```

**Windows:**
Download the installer from https://rclone.org/downloads/

### 5. Connect rclone to Google Drive

Run the interactive setup:
```bash
rclone config
```

Follow these steps exactly:
- Press `n` → new remote
- Name it exactly: `gdrive`
- Type `drive` and press Enter (Google Drive)
- Leave client ID blank → press Enter
- Leave client secret blank → press Enter
- Press `1` for full access scope
- Leave root folder blank → press Enter
- Leave service account blank → press Enter
- Press `y` for auto config → your browser opens
- Sign in with your Gmail account → click Allow
- Press `y` to confirm
- Press `q` to quit config

### 6. Ask the pipeline owner to share the Drive folder with your Gmail

The owner goes to Google Drive, right-clicks the shared folder → Share → adds your Gmail as Editor. Once they do this you have access.

### 7. Update RCLONE_REMOTE in teammate_sync.py

Open `teammate_sync.py` and update this line to match the folder name the owner shared with you:

```python
RCLONE_REMOTE = "gdrive:finance-chatbot-data/cleaned"   # ← use actual folder name
```

### 8. Run the sync watcher

```bash
python teammate_sync.py
```

Leave this running in a terminal. It pulls new files every 60 seconds automatically. Your `cleaned/` folder grows as the pipeline produces files.

---

## Daily Workflow

### Get the latest code
```bash
git pull
```

### Get the latest data (if sync watcher is not running)
```bash
rclone sync gdrive:finance-chatbot-data/cleaned/ ./cleaned/ --update
```

### Start the live sync watcher
```bash
python teammate_sync.py
```

---

## Folder Structure

```
your-repo/
├── pipeline/                        ← all Python scripts (in Git)
│   ├── batch_pipeline.py
│   ├── url_scraping.py
│   ├── normalize_scraped_news.py
│   └── filter_business.py
│
├── teammate_sync.py                 ← run this to receive data (in Git)
├── dvc.yaml                         ← DVC pipeline definition (in Git)
├── .dvc/
│   └── config                       ← DVC config (in Git)
├── .dvcignore                        ← what DVC ignores (in Git)
├── .gitignore                        ← what Git ignores (in Git)
├── DATA_SETUP.md                     ← this file (in Git)
│
├── bigquery_article_results.csv      ← NOT in Git (too large)
├── batches/                          ← NOT in Git (temporary, deleted after processing)
└── cleaned/                          ← NOT in Git (synced via rclone)
```

---

## Troubleshooting

**`rclone config` browser doesn't open**
Run `rclone authorize "drive"` separately, copy the token, and paste it back into config.

**`teammate_sync.py` says "Cannot reach remote"**
- Make sure you named the remote exactly `gdrive` during rclone config
- Make sure the owner has shared the Drive folder with your Gmail
- Double-check `RCLONE_REMOTE` in `teammate_sync.py` matches the actual folder name

**Files appear in Drive but not in your local cleaned/**
Make sure `RCLONE_REMOTE` includes `/cleaned` at the end:
```python
RCLONE_REMOTE = "gdrive:finance-chatbot-data/cleaned"   # correct
RCLONE_REMOTE = "gdrive:finance-chatbot-data"           # wrong — too broad
```

**Sync watcher keeps timing out**
Your internet connection is slow relative to file size. Increase the timeout in `teammate_sync.py`:
```python
timeout=600    # increase from 300 to 600 seconds
```

**`dvc status` shows everything as changed**
This is expected — DVC tracks pointers, not live files. Run `dvc add cleaned/` and `git commit` after a full pipeline run to snapshot that version. Between runs, use rclone for live access.
