import os, csv, sys, time, datetime as dt
import requests

CSV_PATH = os.getenv("CSV_PATH", "captions.csv")
IG_USER_ID = os.getenv("IG_USER_ID")
ACCESS_TOKEN = os.getenv("IG_LONG_LIVED_TOKEN")
GRAPH_BASE = "https://graph.facebook.com/v20.0"

def pick_next_row(rows):
    today = dt.date.today()
    for i, r in enumerate(rows):
        posted = (r.get("posted","").strip().upper() == "TRUE")
        if posted:
            continue
        d = r.get("date","").strip()
        if not d:
            return i
        try:
            row_date = dt.date.fromisoformat(d)
        except Exception:
            continue
        if row_date <= today:
            return i
    return None

def read_rows(path):
    with open(path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames

def write_rows(path, fieldnames, rows):
    with open(path, "w", newline='', encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def create_media(image_url, caption):
    url = f"{GRAPH_BASE}/{IG_USER_ID}/media"
    payload = {
        "image_url": image_url,
        "caption": caption,
        "access_token": ACCESS_TOKEN
    }
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"create_media failed: {r.status_code} {r.text}")
    return r.json()["id"]

def publish_media(creation_id):
    url = f"{GRAPH_BASE}/{IG_USER_ID}/media_publish"
    payload = {"creation_id": creation_id, "access_token": ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"publish_media failed: {r.status_code} {r.text}")
    return r.json()

def main():
    if not IG_USER_ID or not ACCESS_TOKEN:
        print("Missing IG_USER_ID or IG_LONG_LIVED_TOKEN", file=sys.stderr)
        sys.exit(1)
    rows, fields = read_rows(CSV_PATH)
    idx = pick_next_row(rows)
    if idx is None:
        print("No eligible rows today.")
        return
    row = rows[idx]
    image_url = row.get("image_url","").strip()
    caption = row.get("caption","").strip()
    if not image_url:
        print("Row has no image_url; skipping.", file=sys.stderr)
        return
    print(f"Creating media for {image_url}")
    creation_id = create_media(image_url, caption)
    time.sleep(3)
    print("Publishing...")
    out = publish_media(creation_id)
    print(out)
    rows[idx]["posted"] = "TRUE"
    write_rows(CSV_PATH, fields, rows)
    print("Updated CSV.")

if __name__ == "__main__":
    main()
