# Data Setup Guide for Teammates
## Business & Finance RAG Chatbot — Pipeline Data Access

This project uses **DVC** for data version control alongside Git.
Code lives in Git. Data lives in DVC-managed shared storage.
You need both to work on this project.

---

## One-Time Setup (do this once on your machine)

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
pip install dvc dvc-gdrive
```

### 4. Authenticate with shared storage (Google Drive)
```bash
dvc pull
```
Your browser will open and ask you to sign in with Google.
Use the Google account that has access to the shared team Drive folder.
DVC will download all the cleaned CSVs automatically.

That's it. Your `cleaned/` folder now has the same data as everyone else.

---

## Daily Workflow

### Get the latest code AND data
```bash
git pull        # gets latest code + .dvc pointer files
dvc pull        # downloads any new data files that the pipeline produced
```

Run these two commands together every morning before starting work.

### Check what data has changed
```bash
dvc status      # shows which data files are out of date
```

### If you ran the pipeline yourself and want to share new data
```bash
dvc push        # uploads your new cleaned CSVs to shared storage
git add cleaned.dvc
git commit -m "add cleaned batches 10-20"
git push
```

---

## Receiving Files as the Pipeline Produces Them

The pipeline owner's machine syncs each cleaned CSV to Google Drive
the moment it finishes processing. To get files as they appear
(without waiting for a manual `dvc push`), run this sync watcher:

```bash
# Install rclone (one-time)
# Mac:
brew install rclone
# Linux:
sudo apt install rclone

# Configure rclone with Google Drive (one-time, interactive)
rclone config
# Follow prompts → choose Google Drive → name it "gdrive"

# Run the live sync watcher (keep this terminal open)
python teammate_sync.py
```

`teammate_sync.py` checks for new files every 60 seconds and copies
them into your local `cleaned/` folder automatically.

---

## Folder Structure

```
your-repo/
├── pipeline/                   ← all Python scripts (in Git)
│   ├── batch_pipeline.py
│   ├── url_scraping.py
│   ├── normalize_scraped_news.py
│   └── filter_business.py
│
├── dvc.yaml                    ← DVC pipeline definition (in Git)
├── .dvc/
│   └── config                  ← DVC remote config (in Git)
├── .dvcignore                  ← what DVC ignores (in Git)
├── .gitignore                  ← what Git ignores (in Git)
│
├── bigquery_article_results.csv    ← NOT in Git, tracked by DVC
├── batches/                        ← NOT in Git, tracked by DVC
└── cleaned/                        ← NOT in Git, tracked by DVC
```

Files ending in `.dvc` (like `cleaned.dvc`) ARE in Git — they are
tiny pointer files that tell DVC where the real data lives.
Never delete them.

---

## Reproducing the Full Pipeline from Scratch

If you want to run the pipeline yourself end-to-end:

```bash
dvc repro
```

DVC will run only the stages whose inputs have changed since the last run.
To force a full re-run from scratch:

```bash
dvc repro --force
```

To check what would run without actually running it:

```bash
dvc status
```

---

## Troubleshooting

**`dvc pull` asks for Google authentication every time**
Run `dvc remote modify gdrive gdrive_use_service_account false` and re-authenticate once.

**`dvc pull` says "no data to pull"**
The pipeline owner hasn't pushed yet. Ask them to run `dvc push`,
or use the live sync watcher (`teammate_sync.py`) instead.

**A cleaned CSV on your machine differs from a teammate's**
Run `dvc status` — if it shows a mismatch, run `dvc pull` to overwrite
your local copy with the version from shared storage.

**`dvc repro` re-runs a stage you didn't change**
Check if any of the `deps` files for that stage changed — DVC re-runs
a stage whenever any dependency file is modified. This is correct behaviour.
