import requests
import csv
from datetime import datetime, timedelta, timezone
import isodate
import time
from concurrent.futures import ThreadPoolExecutor
import os
import pytz

# ================== CONFIG ==================

API_KEY = os.environ["YOUTUBE_API_KEY"]
BASE_URL = "https://www.googleapis.com/youtube/v3"

BASE_DIR = os.getcwd()
IST = pytz.timezone("Asia/Kolkata")

HOURS_BACK = 1
MAX_DURATION_SEC = 120
SEARCH_QUERIES = ["#shorts", " ", "#short"]
MAX_VIDEOS = 240

OLD_DATA_FOLDER = os.path.join(BASE_DIR, "old_data")
MASTER_VIDEO_FILE = os.path.join(BASE_DIR, "master_video_ids.csv")

os.makedirs(OLD_DATA_FOLDER, exist_ok=True)

# ===========================================


# ---------- SAFE REQUEST ----------
def safe_request(url, params, retries=3):
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request failed (attempt {attempt+1}): {e}")
            time.sleep(1.5 * (attempt + 1))
    return {}


# ---------- TIME ----------
def convert_to_ist(utc_time_str):
    if not utc_time_str:
        return None
    utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(IST).strftime("%Y-%m-%d %I:%M:%S %p")


def generate_filename():
    ts = datetime.now(IST).strftime("%Y-%m-%d_%I-%M-%S_%p")
    return os.path.join(OLD_DATA_FOLDER, f"youtube_data_{ts}.csv")


def get_published_after(hours_back):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()


# ---------- MASTER ----------
def load_master():
    data = {}

    if not os.path.exists(MASTER_VIDEO_FILE):
        return data

    with open(MASTER_VIDEO_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = row.get("videoId")
            if vid:
                data[vid.strip()] = row.get("last_fetched_time", "")

    return data


def save_master(data):
    temp_file = MASTER_VIDEO_FILE + ".tmp"

    with open(temp_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["videoId", "last_fetched_time"])

        for vid, status in data.items():
            writer.writerow([vid, status])

    os.replace(temp_file, MASTER_VIDEO_FILE)


def update_master(existing_data, new_ids):
    for vid in new_ids:
        if vid:
            existing_data[vid] = "NOW"
    save_master(existing_data)


# ---------- SEARCH ----------
def search_videos(published_after, queries):
    video_ids = set()

    def is_valid_video_id(vid):
        return isinstance(vid, str) and len(vid) == 11

    for q in queries:
        next_page_token = None
        start_time = time.time()

        while True:
            if time.time() - start_time > 15:
                break

            params = {
                "part": "id",
                "type": "video",
                "order": "date",
                "publishedAfter": published_after,
                "maxResults": 50,
                "q": q,
                "pageToken": next_page_token,
                "videoDuration": "short",
                "regionCode": "IN",
                "key": API_KEY
            }

            res = safe_request(f"{BASE_URL}/search", params)

            for item in res.get("items", []):
                vid = item.get("id", {}).get("videoId")

                if vid and is_valid_video_id(vid):
                    video_ids.add(vid)

                    if len(video_ids) >= MAX_VIDEOS:
                        return list(video_ids)

            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(0.1)

    return list(video_ids)


# ---------- VIDEO DETAILS ----------
def fetch_video_details_chunk(chunk):
    params = {
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(chunk),
        "key": API_KEY,
        "fields": "items(id,snippet(title,channelId,publishedAt,channelTitle,categoryId),contentDetails(duration),statistics(viewCount,likeCount,commentCount))"
    }
    res = safe_request(f"{BASE_URL}/videos", params)
    return res.get("items", [])


def get_video_details(video_ids):
    all_items = []
    chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for r in executor.map(fetch_video_details_chunk, chunks):
            all_items.extend(r)

    return all_items


# ---------- CHANNEL ----------
def fetch_channel_details(channel_ids):
    all_items = []
    chunks = [channel_ids[i:i+50] for i in range(0, len(channel_ids), 50)]

    for chunk in chunks:
        params = {
            "part": "statistics",
            "id": ",".join(chunk),
            "key": API_KEY
        }

        res = safe_request(f"{BASE_URL}/channels", params)
        all_items.extend(res.get("items", []))
        time.sleep(0.1)

    return {
        i["id"]: i["statistics"].get("subscriberCount")
        for i in all_items
    }


# ---------- DURATION ----------
def parse_duration_safe(d):
    try:
        if not d:
            return None
        seconds = int(isodate.parse_duration(d).total_seconds())
        return seconds if seconds > 0 else None
    except:
        return None


# ---------- PROCESS ----------
def prepare_results(video_items, channel_map):
    results = []

    for item in video_items:
        s = item.get("snippet", {})
        st = item.get("statistics", {})

        vid = item.get("id")
        if not vid or len(vid) != 11:
            continue

        duration = parse_duration_safe(item.get("contentDetails", {}).get("duration"))

        if not duration or duration > MAX_DURATION_SEC:
            continue

        cid = s.get("channelId")

        results.append({
            "videoId": vid,
            "title": s.get("title"),
            "channelId": cid,
            "channelTitle": s.get("channelTitle"),
            "publishedAt": convert_to_ist(s.get("publishedAt")),
            "duration_sec": duration,
            "categoryId": s.get("categoryId"),
            "viewCount": st.get("viewCount"),
            "likeCount": st.get("likeCount"),
            "commentCount": st.get("commentCount"),
            "subscriberCount": channel_map.get(cid)
        })

    return results


# ---------- SAVE ----------
def save_to_csv(data, filename):
    if not data:
        print("⚠️ No data to save")
        return

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"📁 Saved: {filename}")


# ---------- RUN ----------
def run_scraper():
    print("\n==============================")
    print("⏳ RUNNING:", datetime.now(IST).strftime("%Y-%m-%d %I:%M:%S %p"))

    video_ids = search_videos(get_published_after(HOURS_BACK), SEARCH_QUERIES)
    print(f"📥 Video IDs: {len(video_ids)}")

    video_items = get_video_details(video_ids)
    print(f"📦 Video details: {len(video_items)}")

    channel_ids = list(set([
        i.get("snippet", {}).get("channelId")
        for i in video_items
        if i.get("snippet", {}).get("channelId")
    ]))

    channel_map = fetch_channel_details(channel_ids)

    results = prepare_results(video_items, channel_map)
    print(f"✅ Final videos: {len(results)}")

    save_to_csv(results, generate_filename())

    master = load_master()
    new_ids = [r["videoId"] for r in results if r["videoId"] not in master]
    update_master(master, new_ids)

    print("🚀 DONE")


# ---------- ENTRY ----------
if __name__ == "__main__":
    try:
        run_scraper()
    except Exception as e:
        print("❌ Error:", e)

    print("🏁 Finished")