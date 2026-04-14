import requests
import csv
from datetime import datetime
import time
import os
import pytz
import re

# ==========================
# CONFIG
# ==========================

API_KEY = os.environ["YOUTUBE_API_KEY"]

BASE_URL = "https://www.googleapis.com/youtube/v3"

BASE_DIR = os.getcwd()
OLD_DATA_FOLDER = os.path.join(BASE_DIR, "old_data")
MASTER_FILE = os.path.join(BASE_DIR, "master_video_ids.csv")

IST = pytz.timezone("Asia/Kolkata")

# Ensure folder exists
os.makedirs(OLD_DATA_FOLDER, exist_ok=True)


# ==========================
# TIME
# ==========================
def get_current_time_ist():
    return datetime.now(IST).strftime("%Y-%m-%d %I:%M:%S %p")


# ==========================
# FILE NAME
# ==========================
def generate_filename():
    timestamp = datetime.now(IST).strftime("%Y-%m-%d_%I-%M-%S_%p")
    return os.path.join(OLD_DATA_FOLDER, f"youtube_data_{timestamp}.csv")


# ==========================
# CLEAN VIDEO ID (🔥 FIX HERE)
# ==========================
def clean_video_id(raw_id):
    if not raw_id:
        return ""

    vid = raw_id.strip()

    # Remove Excel formula prefix "="
    if vid.startswith("="):
        vid = vid.lstrip("=")

    # Remove quotes Excel might add
    vid = vid.strip('"').strip("'")

    return vid


# ==========================
# VALIDATION (STRONG)
# ==========================
def is_valid_video_id(vid):
    return bool(re.match(r"^[A-Za-z0-9_-]{11}$", vid))


# ==========================
# LOAD + CLEAN MASTER CSV
# ==========================
def load_master():
    if not os.path.exists(MASTER_FILE):
        print("⚠️ master_video_ids.csv not found")
        return []

    clean_rows = []
    removed_count = 0
    fixed_count = 0

    with open(MASTER_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        print("📌 CSV Columns:", reader.fieldnames)

        for row in reader:
            raw_vid = row.get("videoId") or ""
            vid = clean_video_id(raw_vid)

            if raw_vid != vid:
                fixed_count += 1

            if not is_valid_video_id(vid):
                removed_count += 1
                continue

            clean_rows.append({
                "videoId": vid,
                "last_fetched_time": row.get("last_fetched_time", "")
            })

    save_master(clean_rows)

    print(f"🧹 Removed invalid rows: {removed_count}")
    print(f"🛠 Fixed Excel-corrupted IDs: {fixed_count}")

    return clean_rows


# ==========================
# SAVE MASTER CSV (SAFE WRITE)
# ==========================
def save_master(rows):
    temp_file = MASTER_FILE + ".tmp"

    with open(temp_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["videoId", "last_fetched_time"])
        writer.writeheader()
        writer.writerows(rows)

    os.replace(temp_file, MASTER_FILE)


# ==========================
# API CALL
# ==========================
def call_videos_api(ids):
    params = {
        "part": "snippet,statistics",
        "id": ",".join(ids),
        "key": API_KEY
    }

    res = requests.get(f"{BASE_URL}/videos", params=params, timeout=15)
    res.raise_for_status()
    return res.json().get("items", [])


def fetch_video_details(video_ids):
    all_items = []
    chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]

    for chunk in chunks:
        try:
            items = call_videos_api(chunk)
            all_items.extend(items)

        except Exception as e:
            print("⚠️ Chunk failed, retrying individually...", e)

            for vid in chunk:
                try:
                    items = call_videos_api([vid])
                    all_items.extend(items)
                except Exception:
                    print(f"❌ Failed video: {vid}")

        time.sleep(0.1)

    return all_items


# ==========================
# CHANNEL API
# ==========================
def fetch_channel_details(channel_ids):
    all_items = []
    chunks = [channel_ids[i:i+50] for i in range(0, len(channel_ids), 50)]

    for chunk in chunks:
        params = {
            "part": "statistics",
            "id": ",".join(chunk),
            "key": API_KEY
        }

        try:
            res = requests.get(f"{BASE_URL}/channels", params=params, timeout=15)
            res.raise_for_status()
            all_items.extend(res.json().get("items", []))

        except Exception as e:
            print(f"❌ Channel API error: {e}")

        time.sleep(0.1)

    return {
        item["id"]: item["statistics"].get("subscriberCount")
        for item in all_items
    }


# ==========================
# DATA PREP
# ==========================
def prepare_data(items, channel_map):
    data = []
    now_time = get_current_time_ist()

    for item in items:
        s = item.get("snippet", {})
        st = item.get("statistics", {})

        vid = item.get("id")
        if not vid:
            continue

        data.append({
            "videoId": vid,
            "title": s.get("title"),
            "timestamp": now_time,
            "viewCount": st.get("viewCount"),
            "likeCount": st.get("likeCount"),
            "commentCount": st.get("commentCount"),
            "subscriberCount": channel_map.get(s.get("channelId"))
        })

    return data


# ==========================
# SAVE OUTPUT CSV
# ==========================
def save_csv(data):
    if not data:
        print("⚠️ No data to save")
        return

    filename = generate_filename()

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"📁 Saved: {filename}")


# ==========================
# MAIN
# ==========================
def run_scraper():
    print("\n==============================")
    print("⏳ RUNNING SCRAPER:", get_current_time_ist())

    rows = load_master()

    video_ids = [
        r["videoId"]
        for r in rows
        if r["videoId"] and r.get("last_fetched_time") != "NOW"
    ]

    if not video_ids:
        print("⚠️ No valid videos found")
        return

    print(f"🔁 Fetching {len(video_ids)} videos...")

    items = fetch_video_details(video_ids)

    if not items:
        print("⚠️ API returned no valid data")
        return

    channel_ids = list(set([
        item.get("snippet", {}).get("channelId")
        for item in items
        if item.get("snippet", {}).get("channelId")
    ]))

    channel_map = fetch_channel_details(channel_ids)

    data = prepare_data(items, channel_map)
    save_csv(data)

    # reset NOW flags
    for r in rows:
        if r.get("last_fetched_time") == "NOW":
            r["last_fetched_time"] = ""

    save_master(rows)

    print("✅ Scraper completed successfully")


# ==========================
# ENTRY
# ==========================
if __name__ == "__main__":
    try:
        run_scraper()
    except Exception as e:
        print("❌ Fatal error:", e)

    print("🏁 Script finished.")
