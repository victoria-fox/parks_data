import os
import time
import logging
from datetime import datetime, timezone

import requests
import pg8000.native

# --- Config ---
DATABASE_URL = os.environ["DATABASE_URL"]
PARK_IDS = [5, 6, 7, 8, 16, 17]  # EPCOT, Magic Kingdom, Hollywood Studios, Animal Kingdom, Disneyland, DCA
INTERVAL_SECONDS = 300  # 5 minutes
API_BASE = "https://queue-times.com/parks/{park_id}/queue_times.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def upsert_ride(cur, ride_id, park_id, name, land_id, land_name):
    cur.execute("""
        INSERT INTO rides (id, park_id, name, land_id, land_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name      = EXCLUDED.name,
            land_id   = EXCLUDED.land_id,
            land_name = EXCLUDED.land_name,
            updated_at = NOW()
    """, (ride_id, park_id, name, land_id, land_name))


def fetch_and_store(park_id):
    url = API_BASE.format(park_id=park_id)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Park {park_id}: fetch failed — {e}")
        return

    collected_at = datetime.now(timezone.utc)
    rides_data = []  # (ride_id, park_id, is_open, wait_minutes, collected_at, source_updated_at)

    # Rides grouped under lands
    for land in data.get("lands", []):
        land_id = land.get("id")
        land_name = land.get("name")
        for ride in land.get("rides", []):
            ride_id = ride["id"]
            rides_data.append((
                ride_id,
                park_id,
                ride.get("is_open"),
                ride.get("wait_time"),
                collected_at,
                ride.get("last_updated"),
                ride["name"],
                land_id,
                land_name,
            ))

    # Top-level rides (not in a land)
    for ride in data.get("rides", []):
        ride_id = ride["id"]
        rides_data.append((
            ride_id,
            park_id,
            ride.get("is_open"),
            ride.get("wait_time"),
            collected_at,
            ride.get("last_updated"),
            ride["name"],
            None,
            None,
        ))

    if not rides_data:
        log.warning(f"Park {park_id}: no rides returned")
        return

    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                for row in rides_data:
                    ride_id, park_id_, is_open, wait_time, coll, src_updated, name, land_id, land_name = row
                    upsert_ride(cur, ride_id, park_id_, name, land_id, land_name)

                execute_values(cur, """
                    INSERT INTO wait_times (ride_id, park_id, is_open, wait_minutes, collected_at, source_updated_at)
                    VALUES %s
                """, [
                    (r[0], r[1], r[2], r[3], r[4], r[5])
                    for r in rides_data
                ])

        conn.close()
        log.info(f"Park {park_id}: stored {len(rides_data)} rides")
    except Exception as e:
        log.error(f"Park {park_id}: DB error — {e}")


def run():
    log.info(f"Collector starting. Parks: {PARK_IDS}, interval: {INTERVAL_SECONDS}s")
    while True:
        start = time.time()
        for park_id in PARK_IDS:
            fetch_and_store(park_id)
            time.sleep(1)  # gentle pacing between park requests
        elapsed = time.time() - start
        sleep_for = max(0, INTERVAL_SECONDS - elapsed)
        log.info(f"Cycle done in {elapsed:.1f}s. Sleeping {sleep_for:.0f}s.")
        time.sleep(sleep_for)


if __name__ == "__main__":
    run()
