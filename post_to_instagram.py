#!/usr/bin/env python3
"""
Post queued images to Instagram using the Instagram Graph API,
reading from captions.csv.

Requirements:
    pip install requests

Environment variables:
    IG_USER_ID        -> your Instagram Business Account ID (1784...)
    IG_ACCESS_TOKEN   -> your long-lived Instagram access token

Input file:
    captions.csv in the same folder, with columns:
        date,image_url,Category,Post No.,caption,posted,_to_post,
        _date_parsed,posted_at,instagram_media_id,error

Usage:
    python post_to_instagram.py --limit 1
    python post_to_instagram.py --limit 5
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from typing import List, Dict

import requests


GRAPH_FACEBOOK_BASE = "https://graph.facebook.com/v21.0"
CSV_PATH = "captions.csv"


def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: environment variable {name} is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def load_rows(csv_path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file '{csv_path}' not found.", file=sys.stderr)
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Just sanity-check a couple of key headers
        required = {"image_url", "caption", "posted"}
        if not required.issubset(reader.fieldnames or []):
            print(
                f"ERROR: CSV must contain at least these columns: {', '.join(required)}",
                file=sys.stderr,
            )
            print(f"Found columns: {reader.fieldnames}", file=sys.stderr)
            sys.exit(1)

        for row in reader:
            rows.append(row)

    return rows


def save_rows(csv_path: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    tmp_path = csv_path + ".tmp"

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    os.replace(tmp_path, csv_path)


def is_truthy(value: str) -> bool:
    v = (value or "").strip().lower()
    return v in ("1", "true", "yes", "y", "t")


def is_falsy(value: str) -> bool:
    v = (value or "").strip().lower()
    return v in ("0", "false", "no", "n", "f")


def row_is_pending(row: Dict[str, str]) -> bool:
    posted = row.get("posted", "")
    to_post_flag = row.get("_to_post", "")

    # Already posted?
    if is_truthy(posted):
        return False

    # Explicitly marked "do NOT post"?
    if is_falsy(to_post_flag):
        return False

    # Otherwise, treat it as pending
    return True


def create_media_container(ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    """
    Step 1: Create a media container.
    """
    endpoint = f"{GRAPH_FACEBOOK_BASE}/{ig_user_id}/media"
    params = {
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    }

    resp = requests.post(endpoint, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Error creating media container: {resp.status_code} {resp.text}"
        )

    data = resp.json()
    creation_id = data.get("id")
    if not creation_id:
        raise RuntimeError(f"No creation_id in response: {data}")

    return creation_id


def wait_for_container_ready(creation_id: str, access_token: str,
                             max_attempts: int = 10, delay: float = 2.0) -> None:
    """
    Poll the container status until it's FINISHED or we give up.
    """
    endpoint = f"{GRAPH_FACEBOOK_BASE}/{creation_id}"
    params = {
        "fields": "status_code",
        "access_token": access_token,
    }

    for attempt in range(1, max_attempts + 1):
        resp = requests.get(endpoint, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Error checking container status: {resp.status_code} {resp.text}"
            )
        data = resp.json()
        status = data.get("status_code")
        if status == "FINISHED":
            return
        elif status == "ERROR":
            raise RuntimeError(f"Container {creation_id} ended in ERROR: {data}")

        time.sleep(delay)

    raise RuntimeError(
        f"Container {creation_id} did not reach FINISHED after {max_attempts} attempts"
    )


def publish_media(ig_user_id: str, access_token: str, creation_id: str) -> str:
    """
    Step 2: Publish the media container.
    """
    endpoint = f"{GRAPH_FACEBOOK_BASE}/{ig_user_id}/media_publish"
    params = {
        "creation_id": creation_id,
        "access_token": access_token,
    }

    resp = requests.post(endpoint, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Error publishing media: {resp.status_code} {resp.text}"
        )

    data = resp.json()
    ig_media_id = data.get("id")
    if not ig_media_id:
        raise RuntimeError(f"No media id in response: {data}")

    return ig_media_id


def post_single_row(ig_user_id: str, access_token: str, row: Dict[str, str]) -> str:
    """
    Full flow for one row: create container -> wait -> publish.
    """
    image_url = row["image_url"]
    caption = row.get("caption", "")

    print(f"  • Creating media container for: {image_url}")
    creation_id = create_media_container(ig_user_id, access_token, image_url, caption)

    print(f"    Container created: {creation_id}, waiting to finish…")
    wait_for_container_ready(creation_id, access_token)

    print("    Publishing media…")
    media_id = publish_media(ig_user_id, access_token, creation_id)
    print(f"    ✅ Published! Media ID: {media_id}")
    return media_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Post queued images to Instagram via Graph API (captions.csv).")
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum number of new posts to publish (default: 1)",
    )
    args = parser.parse_args()

    ig_user_id = get_env_var("IG_USER_ID")
    access_token = get_env_var("IG_ACCESS_TOKEN")

    rows = load_rows(CSV_PATH)
    pending_indices = [i for i, r in enumerate(rows) if row_is_pending(r)]

    if not pending_indices:
        print("No pending posts found in captions.csv (all either posted or _to_post is falsy).")
        return

    print(f"Found {len(pending_indices)} pending rows. Will publish up to {args.limit}.\n")

    posted_count = 0
    now_iso = lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for idx in pending_indices:
        if posted_count >= args.limit:
            break

        row = rows[idx]
        print(f"Posting row #{idx + 1} (Post No.: {row.get('Post No.', '')})")

        try:
            media_id = post_single_row(ig_user_id, access_token, row)
            # Mark as posted
            row["posted"] = "1"
            row["posted_at"] = now_iso()
            row["instagram_media_id"] = media_id
            row["error"] = ""
            posted_count += 1
            print("")
        except Exception as e:
            msg = str(e)
            print(f"    ❌ Failed to publish this row: {msg}", file=sys.stderr)
            # Record the error but don't mark as posted
            row["error"] = msg[:500]  # avoid insane length

    save_rows(CSV_PATH, rows)
    print(f"Done. Successfully published {posted_count} post(s).")


if __name__ == "__main__":
    main()
