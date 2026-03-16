import fastf1
import pandas as pd
import os
import time
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# -------------------------------------------------------
# CONFIGURATION  — edit these to match your environment
# -------------------------------------------------------
START_YEAR   = 2018
END_YEAR     = 2024          # inclusive
OUTPUT_DIR   = "f1_data"     # same folder as your Jolpica CSVs
CACHE_DIR    = "cache"       # FastF1 local cache
DELAY        = 1.5           # seconds between session loads (be polite to the API)

# Session types to extract.  FP1/FP2/FP3 = practice sessions.
SESSION_TYPES = ["FP1", "FP2", "FP3", "Q", "R"]

# Telemetry channels that will be saved for every driver lap
TELEMETRY_CHANNELS = [
    "Time", "Distance", "Speed", "Throttle", "Brake",
    "nGear", "RPM", "DRS", "X", "Y", "Z"
]

# -------------------------------------------------------
# SETUP
# -------------------------------------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR,  exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)


# -------------------------------------------------------
# UTILITIES  (same style as your Jolpica script)
# -------------------------------------------------------
def save_csv(df: pd.DataFrame, name: str):
    """Save a DataFrame to CSV, same convention as Jolpica script."""
    if df is not None and not df.empty:
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  💾 Saved {len(df):,} rows → {path}")
    else:
        print(f"  ⚠️  No data to save for {name}")


def safe_load_session(year: int, round_num: int, session_type: str, retries: int = 3):
    """
    Load a FastF1 session with retry logic.
    Returns the session object or None on failure.
    """
    wait = DELAY
    for attempt in range(1, retries + 1):
        try:
            session = fastf1.get_session(year, round_num, session_type)
            session.load(telemetry=True, laps=True, weather=True, messages=False)
            return session
        except Exception as e:
            print(f"    ⚠️  Attempt {attempt}/{retries} failed for "
                  f"{year} Rd{round_num} {session_type}: {e}")
            time.sleep(wait)
            wait *= 2   # exponential backoff — mirrors your Jolpica script
    return None


def session_meta(session, year: int, round_num: int, session_type: str) -> dict:
    """Build the race-level metadata dict that prefixes every telemetry row."""
    event = session.event
    return {
        "season":         year,
        "round":          round_num,
        "session_type":   session_type,
        "raceName":       event.get("EventName", ""),
        "circuit_name":   event.get("Location",  ""),
        "circuit_country":event.get("Country",   ""),
        "event_date":     str(event.get("EventDate", "")),
    }


# -------------------------------------------------------
# CORE EXTRACTORS
# -------------------------------------------------------
def extract_laps(session, meta: dict) -> pd.DataFrame:
    """
    Pull per-lap summary data — mirrors Jolpica's Laps.csv in structure.
    Includes: lap times, sector times, tyre compound, tyre life, pit flags,
    speed traps, track status.
    Joins cleanly to your Jolpica Laps.csv on season + round + driver_id.
    """
    laps = session.laps.copy()
    if laps.empty:
        return pd.DataFrame()

    # Flatten driver info into usable column names
    laps = laps.rename(columns={
        "Driver":       "driver_id",
        "DriverNumber": "driver_number",
        "Team":         "team",
        "LapNumber":    "lap_number",
        "LapTime":      "lap_time",
        "Sector1Time":  "sector1_time",
        "Sector2Time":  "sector2_time",
        "Sector3Time":  "sector3_time",
        "Compound":     "tyre_compound",
        "TyreLife":     "tyre_life",
        "FreshTyre":    "fresh_tyre",
        "PitInTime":    "pit_in_time",
        "PitOutTime":   "pit_out_time",
        "SpeedI1":      "speed_trap_I1",
        "SpeedI2":      "speed_trap_I2",
        "SpeedFL":      "speed_trap_FL",
        "SpeedST":      "speed_trap_ST",
        "TrackStatus":  "track_status",
        "IsPersonalBest": "is_personal_best",
        "Position":     "position",
    })

    # Convert timedelta columns to total seconds (easier to work with in CSVs)
    for col in ["lap_time", "sector1_time", "sector2_time", "sector3_time",
                "pit_in_time", "pit_out_time"]:
        if col in laps.columns:
            laps[col] = pd.to_timedelta(laps[col], errors="coerce").dt.total_seconds()

    # Attach session-level metadata (season, round, raceName, etc.)
    for k, v in meta.items():
        laps[k] = v

    return laps.reset_index(drop=True)


def extract_telemetry(session, meta: dict) -> pd.DataFrame:
    """
    Pull car telemetry for EVERY driver and EVERY lap in the session.
    Returns one big DataFrame with columns:
      season, round, session_type, raceName, circuit_name, circuit_country,
      driver_id, driver_number, team, lap_number,
      Time, Distance, Speed, Throttle, Brake, nGear, RPM, DRS, X, Y, Z
    """
    all_rows = []
    laps = session.laps

    if laps.empty:
        return pd.DataFrame()

    drivers = laps["Driver"].unique()

    for driver in drivers:
        driver_laps = laps.pick_driver(driver)
        team        = driver_laps["Team"].iloc[0]       if not driver_laps.empty else ""
        drv_number  = driver_laps["DriverNumber"].iloc[0] if not driver_laps.empty else ""

        for _, lap in driver_laps.iterlaps():
            lap_number = lap["LapNumber"]
            try:
                tel = lap.get_car_data().add_distance()
                if tel.empty:
                    continue

                # Keep only the channels we care about
                keep = [c for c in TELEMETRY_CHANNELS if c in tel.columns]
                tel  = tel[keep].copy()

                # Convert Time (timedelta) to seconds
                if "Time" in tel.columns:
                    tel["Time"] = tel["Time"].dt.total_seconds()

                # Tag every row with driver + lap context
                tel["driver_id"]     = driver
                tel["driver_number"] = drv_number
                tel["team"]          = team
                tel["lap_number"]    = lap_number

                # Tag every row with session metadata
                for k, v in meta.items():
                    tel[k] = v

                all_rows.append(tel)

            except Exception as e:
                # Don't let a single bad lap abort the whole session
                print(f"    ⚠️  Telemetry error driver={driver} lap={lap_number}: {e}")
                continue

    if not all_rows:
        return pd.DataFrame()

    return pd.concat(all_rows, ignore_index=True)


def extract_weather(session, meta: dict) -> pd.DataFrame:
    """Weather snapshots for the session (air temp, track temp, humidity, wind, rain)."""
    try:
        weather = session.weather_data.copy()
        if weather.empty:
            return pd.DataFrame()
        if "Time" in weather.columns:
            weather["Time"] = weather["Time"].dt.total_seconds()
        for k, v in meta.items():
            weather[k] = v
        return weather.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


# -------------------------------------------------------
# SESSION SCHEDULE HELPER
# -------------------------------------------------------
def get_rounds_for_year(year: int) -> list[int]:
    """Return all valid round numbers for a given season."""
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        return schedule["RoundNumber"].tolist()
    except Exception as e:
        print(f"  ⚠️  Could not fetch schedule for {year}: {e}")
        return []


# -------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------
def main():
    print("🏎️  FastF1 Telemetry Extractor")
    print(f"   Seasons : {START_YEAR} → {END_YEAR}")
    print(f"   Sessions: {', '.join(SESSION_TYPES)}")
    print(f"   Output  : {os.path.abspath(OUTPUT_DIR)}\n")

    all_laps      = []
    all_telemetry = []
    all_weather   = []

    years = range(START_YEAR, END_YEAR + 1)

    for year in tqdm(years, desc="Seasons"):
        rounds = get_rounds_for_year(year)
        if not rounds:
            continue

        for round_num in tqdm(rounds, desc=f"  {year} rounds", leave=False):
            for session_type in SESSION_TYPES:
                print(f"\n  → {year} Rd{round_num:02d} [{session_type}]")

                session = safe_load_session(year, round_num, session_type)
                if session is None:
                    print(f"    ✗ Skipped (could not load session)")
                    continue

                meta = session_meta(session, year, round_num, session_type)

                # --- Laps ---
                laps_df = extract_laps(session, meta)
                if not laps_df.empty:
                    all_laps.append(laps_df)
                    print(f"    ✓ Laps     : {len(laps_df):,} rows")

                # --- Telemetry ---
                tel_df = extract_telemetry(session, meta)
                if not tel_df.empty:
                    all_telemetry.append(tel_df)
                    print(f"    ✓ Telemetry: {len(tel_df):,} rows")

                # --- Weather ---
                wx_df = extract_weather(session, meta)
                if not wx_df.empty:
                    all_weather.append(wx_df)
                    print(f"    ✓ Weather  : {len(wx_df):,} rows")

                time.sleep(DELAY)   # be polite to the API

    # -------------------------------------------------------
    # SAVE — one big CSV per data type, same as Jolpica script
    # -------------------------------------------------------
    print("\n📦 Saving combined CSVs...")

    save_csv(
        pd.concat(all_laps,      ignore_index=True) if all_laps      else pd.DataFrame(),
        "FF1_Laps"
    )
    save_csv(
        pd.concat(all_telemetry, ignore_index=True) if all_telemetry else pd.DataFrame(),
        "FF1_Telemetry"
    )
    save_csv(
        pd.concat(all_weather,   ignore_index=True) if all_weather   else pd.DataFrame(),
        "FF1_Weather"
    )

    print("\n✅ All FastF1 data successfully exported!")
    print(f"📂 Output directory: {os.path.abspath(OUTPUT_DIR)}")

    print("""
─────────────────────────────────────────────────
HOW TO JOIN WITH YOUR JOLPICA CSVs
─────────────────────────────────────────────────
FF1_Laps.csv        → join on  season + round + driver_id
FF1_Telemetry.csv   → join on  season + round + driver_id + lap_number
FF1_Weather.csv     → join on  season + round + session_type  (time-based)

Jolpica counterparts:
  Results.csv   → season + round + driver_id  (finishing positions, points)
  Qualifying.csv→ season + round + driver_id  (Q1/Q2/Q3 times)
  PitStops.csv  → season + round + driver_id  (lap number, duration)
  Laps.csv      → season + round + driver_id + lap_number (lap-level times)
─────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()
