#!/usr/bin/env python3
"""
Instagram Auto Poster (CSV-driven) — NEXT-SCHEDULE MODE

CSV supported (your current format is fine):

    date,image_url,Category,Post No,caption,posted

Also supported:

    filename,caption,posted

Posting rules (default):
- **Next scheduled** post only. The script finds the earliest row with `posted` falsey
  and a `date` ≥ today. If there are no future-dated rows, it falls back to the
  earliest past-dated unposted row. If there is no `date` column, it posts the
  first unposted row.
- After success, marks `posted=TRUE`, records `posted_at` and `instagram_media_id`.

Override:
- `--all`  : post ALL matching rows (like the previous behavior).
- `--limit N` works in both modes; in next-schedule mode it limits how many
  consecutive “next” rows to post (usually 1).
- `--dry-run` to simulate without uploading.
- `--category …`, `--start-date …`, `--end-date …` still work.

Auth:
- Set environment variables IG_USERNAME and IG_PASSWORD.
- Session cached in `.ig_session.json`.

Examples:
    # Post today’s (or next) scheduled item only
    python3 post_to_instagram.py

    # Post two upcoming scheduled items
    python3 post_to_instagram.py --limit 2

    # Old behavior (post everything unposted)
    python3 post_to_instagram.py --all

"""
from __future__ import annotations
import argparse
import contextlib
import io
import os
import sys
import tempfile
import time
import shutil
import json
from datetime import datetime, date
from typing import Optional

import pandas as pd
from PIL import Image

try:
    from instagrapi import Client
    from instagrapi.exceptions import LoginRequired
except Exception as e:
    print("ERROR: instagrapi is not installed. Run: pip install instagrapi", file=sys.stderr)
    raise


# ---------- Config ----------
DEFAULT_CSV = "captions.csv"
DEFAULT_IMAGES_DIR = "images"
SESSION_FILE = ".ig_session.json"
BACKUP_SUFFIX = ".bak"
MAX_SIDE = 1350  # Instagram-friendly max pixel side


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Instagram CSV Poster (next-schedule mode)")
    p.add_argument("--csv", default=DEFAULT_CSV, help="Path to CSV file")
    p.add_argument("--images-dir", default=DEFAULT_IMAGES_DIR, help="Local images directory for filename lookups")
    p.add_argument("--dry-run", action="store_true", help="Do everything except the actual upload")
    p.add_argument("--limit", type=int, default=1, help="Max number of posts to attempt this run (default 1)")
    p.add_argument("--all", action="store_true", help="Post ALL matching rows instead of just the next scheduled one(s)")
    p.add_argument("--category", default=None, help="Filter by Category==value (case-insensitive)")
    p.add_argument("--start-date", default=None, help="Only include rows with date >= this (YYYY-MM-DD)")
    p.add_argument("--end-date", default=None, help="Only include rows with date <= this (YYYY-MM-DD)")
    return p.parse_args()


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    for col in ["posted", "caption"]:
        if col not in df.columns:
            df[col] = ""

    def is_falsey(v: str) -> bool:
        v = (v or "").strip().lower()
        return v in {"", "false", "0", "no", "n"}

    df["_to_post"] = df["posted"].apply(is_falsey)

    if "date" in df.columns:
        with contextlib.suppress(Exception):
            df["_date_parsed"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        df["_date_parsed"] = pd.NaT

    return df


def save_csv_atomically(df: pd.DataFrame, path: str) -> None:
    if os.path.exists(path) and not os.path.exists(path + BACKUP_SUFFIX):
        shutil.copy2(path, path + BACKUP_SUFFIX)

    tmp_fd, tmp_path = tempfile.mkstemp(prefix="captions_", suffix=".csv")
    os.close(tmp_fd)
    try:
        df.to_csv(tmp_path, index=False)
        shutil.move(tmp_path, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)


def resolve_image_path(row: pd.Series, images_dir: str) -> tuple[str, bool]:
    url = row.get("image_url", "").strip()
    fn = row.get("filename", "").strip()

    if url:
        if url.lower().startswith(("http://", "https://")):
            import urllib.request
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(url)[1] or ".jpg")
            tmp.close()
            urllib.request.urlretrieve(url, tmp.name)
            return tmp.name, True
        if os.path.isabs(url):
            return url, False
        return os.path.join(images_dir, url), False

    if fn:
        if os.path.isabs(fn):
            return fn, False
        return os.path.join(images_dir, fn), False

    raise ValueError("Row is missing both image_url and filename")


def prepare_image_for_instagram(path: str) -> str:
    with Image.open(path) as im:
        w, h = im.size
        max_side = max(w, h)
        if max_side <= MAX_SIDE:
            return path
        scale = MAX_SIDE / float(max_side)
        new_size = (int(w * scale), int(h * scale))
        out = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(path)[1] or ".jpg")
        out_path = out.name
        out.close()
        im.resize(new_size, Image.LANCZOS).save(out_path, quality=95, optimize=True)
        return out_path


def get_client() -> Client:
    username = os.getenv("IG_USERNAME")
    password = os.getenv("IG_PASSWORD")
    if not username or not password:
        print("Please set IG_USERNAME and IG_PASSWORD environment variables.", file=sys.stderr)
        sys.exit(1)

    cl = Client()

    if os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
            cl.login(username, password)
            return cl
        except Exception:
            pass

    cl.login(username, password)
    with contextlib.suppress(Exception):
        cl.dump_settings(SESSION_FILE)
    return cl


def normalize_caption(text: str) -> str:
    text = (text or "").strip()
    return text[:2200]


def parse_date_or_none(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        print(f"Warning: could not parse date '{s}', ignoring filter.")
        return None


def pick_next_scheduled_rows(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Return up to `limit` rows representing the next scheduled unposted items.
    Prefers future (≥ today), else falls back to earliest past.
    If no date column, picks the first unposted rows by CSV order.
    """
    today = datetime.now().date()
    base = df[df["_to_post"]].copy()

    if "_date_parsed" not in base.columns or base["_date_parsed"].isna().all():
        return base.head(limit)

    fut = base[base["_date_parsed"] >= today].sort_values(["_date_parsed"]).head(limit)
    if not fut.empty:
        return fut

    past = base[base["_date_parsed"] < today].sort_values(["_date_parsed"]).head(limit)
    return past


def main():
    args = parse_args()
    df = load_csv(args.csv)

    # Global filter mask
    mask = df["_to_post"]

    if args.category and "Category" in df.columns:
        mask &= df["Category"].str.strip().str.lower().eq(args.category.strip().lower())

    start_d = parse_date_or_none(args.start_date)
    end_d = parse_date_or_none(args.end_date)

    if start_d is not None and "_date_parsed" in df.columns:
        mask &= df["_date_parsed"].apply(lambda d: (pd.notna(d) and d >= start_d))
    if end_d is not None and "_date_parsed" in df.columns:
        mask &= df["_date_parsed"].apply(lambda d: (pd.notna(d) and d <= end_d))

    filtered = df[mask].copy()

    if filtered.empty:
        print("No rows to post. (Either all are posted already or filters removed them.)")
        return

    if args.all:
        candidates = filtered
    else:
        candidates = pick_next_scheduled_rows(filtered, limit=args.limit)

    if candidates.empty:
        print("No eligible 'next' rows found.")
        return

    print(f"Found {len(candidates)} post(s) to process.")

    cl = None
    if not args.dry_run:
        cl = get_client()

    successes = 0

    for idx, row in candidates.iterrows():
        try:
            img_path, is_temp1 = resolve_image_path(row, args.images_dir)
            prep_path = prepare_image_for_instagram(img_path)
            caption = normalize_caption(row.get("caption", ""))

            human_date = row.get("date", "")
            print(f"\nPosting: {img_path}  (date={human_date})\nCaption: {caption[:80]}{'...' if len(caption)>80 else ''}")

            if args.dry_run:
                media_pk = "DRY_RUN"
                print("[dry-run] Skipping upload.")
            else:
                media = cl.photo_upload(prep_path, caption)
                media_pk = getattr(media, "pk", None) or ""
                time.sleep(2)

            df.at[idx, "posted"] = "TRUE"
            df.at[idx, "posted_at"] = datetime.now().isoformat(timespec="seconds")
            df.at[idx, "instagram_media_id"] = str(media_pk)
            if "error" in df.columns:
                df.at[idx, "error"] = ""
            successes += 1

        except Exception as e:
            err = str(e)
            print(f"ERROR posting row {idx}: {err}", file=sys.stderr)
            if "error" not in df.columns:
                df["error"] = ""
            df.at[idx, "error"] = err
        finally:
            with contextlib.suppress(Exception):
                if 'is_temp1' in locals() and is_temp1 and os.path.exists(img_path):
                    os.remove(img_path)
            with contextlib.suppress(Exception):
                if 'prep_path' in locals() and prep_path not in (img_path,) and os.path.exists(prep_path):
                    os.remove(prep_path)

        save_csv_atomically(df, args.csv)

    print(f"\nDone. Successfully processed {successes} post(s). CSV updated.")


if __name__ == "__main__":
    main()
