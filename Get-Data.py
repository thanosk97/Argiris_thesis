import requests
import pandas as pd
import os
import time
from tqdm import tqdm

BASE_URL = "https://api.jolpi.ca/ergast/f1"
OUTPUT_DIR = "f1_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
BATCH_SIZE = 1000
REQUEST_TIMEOUT = 60  # Increased timeout
RETRIES = 5  # Retry on failure
DELAY = 2  # Delay between requests in seconds

# -----------------------------
# UTILITIES
# -----------------------------
def safe_get_json(url, retries=RETRIES):
    """GET request with retries and timeout"""
    for i in range(retries):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"⚠️ HTTP {r.status_code} for {url}")
        except Exception as e:
            print(f"⚠️ Error fetching {url} (attempt {i+1}/{retries}): {e}")
        time.sleep(2)
    return None


def flatten(df):
    """Flatten nested column names for CSV"""
    df.columns = [
        c.replace(".", "_").replace("Driver_", "driver_")
         .replace("Constructor_", "constructor_")
         .replace("Circuit_", "circuit_")
         .replace("Time_", "time_")
         .replace("AverageSpeed_", "avgSpeed_")
        for c in df.columns
    ]
    return df


def save_csv(df, name):
    """Save DataFrame to CSV"""
    if not df.empty:
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"💾 Saved {len(df)} rows → {path}")
    else:
        print(f"⚠️ No data for {name}")


# -----------------------------
# BASIC ENDPOINTS
# -----------------------------
def fetch_paginated(url, key_path):
    """Fetch paginated data for simple endpoints like seasons/drivers/etc"""
    offset = 0
    all_data = []
    while True:
        sep = "&" if "?" in url else "?"
        paged = f"{url}{sep}limit={BATCH_SIZE}&offset={offset}"
        data = safe_get_json(paged)
        if not data:
            break
        items = data.get("MRData", {})
        for k in key_path:
            items = items.get(k, [])
        if not items:
            break
        all_data.extend(items)
        offset += BATCH_SIZE
        if len(items) < BATCH_SIZE:
            break
    df = pd.json_normalize(all_data)
    return flatten(df)


def fetch_all_seasons():
    return fetch_paginated(f"{BASE_URL}/seasons.json", ["SeasonTable", "Seasons"])

def fetch_all_drivers():
    return fetch_paginated(f"{BASE_URL}/drivers.json", ["DriverTable", "Drivers"])

def fetch_all_constructors():
    return fetch_paginated(f"{BASE_URL}/constructors.json", ["ConstructorTable", "Constructors"])

def fetch_all_circuits():
    return fetch_paginated(f"{BASE_URL}/circuits.json", ["CircuitTable", "Circuits"])


# -----------------------------
# PER-SEASON / PER-ROUND DATASETS
# -----------------------------
def get_races_for_season(year):
    """Get all races for a season"""
    url = f"{BASE_URL}/{year}.json"
    data = safe_get_json(url)
    if not data:
        return []
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    return races


def fetch_race_data(year, dataset):
    """Fetch dataset for all rounds in a season"""
    races = get_races_for_season(year)
    all_rows = []

    for race in races:
        round_num = race.get("round")
        race_name = race.get("raceName")
        date = race.get("date")

        # Round-specific endpoint
        url = f"{BASE_URL}/{year}/{round_num}/{dataset}.json"
        data = safe_get_json(url)
        if not data:
            print(f"⚠️ Skipping {dataset} for {year} round {round_num}")
            continue

        race_data = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        for r in race_data:
            race_meta = {
                "season": year,
                "round": round_num,
                "raceName": race_name,
                "date": date,
                "circuit_id": r.get("Circuit", {}).get("circuitId"),
                "circuit_name": r.get("Circuit", {}).get("circuitName"),
                "circuit_location": r.get("Circuit", {}).get("Location", {}).get("locality"),
                "circuit_country": r.get("Circuit", {}).get("Location", {}).get("country"),
            }

            # Flatten nested keys
            nested_keys = ["Results", "QualifyingResults", "SprintResults", "PitStops", "Laps"]
            for key in nested_keys:
                if key in r:
                    for item in r[key]:
                        flat = pd.json_normalize(item)
                        flat = flatten(flat)
                        for k, v in race_meta.items():
                            flat[k] = v
                        all_rows.append(flat)
        time.sleep(DELAY)  # delay between rounds

    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def fetch_all_years(dataset, start_year=2024, end_year=2025):
    """Fetch a dataset for all seasons"""
    all_data = []
    for year in tqdm(range(start_year, end_year + 1), desc=f"Fetching {dataset}"):
        df = fetch_race_data(year, dataset)
        if not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    print("🏎️ Fetching complete F1 dataset by season...\n")

    datasets = {
        "Seasons": fetch_all_seasons(),
        "Drivers": fetch_all_drivers(),
        "Constructors": fetch_all_constructors(),
        "Circuits": fetch_all_circuits(),
        "Results": fetch_all_years("results"),
        "Qualifying": fetch_all_years("qualifying"),
        "Sprint": fetch_all_years("sprint"),
        "DriverStandings": fetch_all_years("driverStandings"),
        "ConstructorStandings": fetch_all_years("constructorStandings"),
        "PitStops": fetch_all_years("pitstops"),
        "Laps": fetch_all_years("laps"),
    }

    for name, df in datasets.items():
        save_csv(df, name)

    print("\n✅ All data successfully exported!")
    print(f"📂 Output directory: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
