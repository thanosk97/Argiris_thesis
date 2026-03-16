import time
from tqdm import tqdm
import fastf1
import pandas as pd

fastf1.Cache.enable_cache('cache/')

SESSION_TYPES = ['FP2', 'FP3', 'Q']
YEARS = range(2018, 2025)
DELAY = 1.5
RETRIES = 5

# ── checkpoint: skip already-processed (season, round, session) combinations
CHECKPOINT_FILE = 'f1_data/FF1_Features.csv'


def load_checkpoint():
    """Load already-extracted features so we don't re-run completed sessions."""
    if os.path.exists(CHECKPOINT_FILE):
        df = pd.read_csv(CHECKPOINT_FILE)
        done = set(zip(df['season'], df['round'], df['session_type']))
        print(f"  ✓ Checkpoint loaded — {len(done)} sessions already done")
        return df, done
    return pd.DataFrame(), set()


def get_schedule_safe(year, retries=RETRIES):
    """Fetch event schedule with retries and exponential backoff."""
    wait = DELAY
    for attempt in range(1, retries + 1):
        try:
            schedule = fastf1.get_event_schedule(year, include_testing=False)
            if schedule is not None and not schedule.empty:
                return schedule
        except Exception as e:
            print(f"  ⚠ Schedule fetch failed for {year} "
                  f"(attempt {attempt}/{retries}): {e}")
        time.sleep(wait)
        wait *= 2  # exponential backoff
    print(f"  ✗ Could not load schedule for {year} after {retries} attempts — skipping year")
    return None


def extract_session_features(year, round_num, session_type):
    try:
        session = fastf1.get_session(year, round_num, session_type)
        session.load(telemetry=True, laps=True, weather=False)
    except Exception as e:
        print(f"  ✗ Could not load {year} Rd{round_num} {session_type}: {e}")
        return pd.DataFrame()

    try:
        laps = session.laps
        if laps is None or laps.empty:
            print(f"  ⚠ No lap data: {year} Rd{round_num} {session_type}")
            return pd.DataFrame()
    except Exception as e:
        print(f"  ⚠ Lap data unavailable: {year} Rd{round_num} {session_type}: {e}")
        return pd.DataFrame()

    clean_laps = laps[
        (laps['TrackStatus'] == '1') &
        (laps['PitOutTime'].isna()) &
        (laps['PitInTime'].isna())
    ]

    rows = []
    for driver in clean_laps['Driver'].unique():
        driver_laps = clean_laps.pick_drivers(driver)
        if len(driver_laps) < 3:
            continue

        lap_times = driver_laps['LapTime'].dt.total_seconds().dropna()
        s1 = driver_laps['Sector1Time'].dt.total_seconds().dropna()
        s2 = driver_laps['Sector2Time'].dt.total_seconds().dropna()
        s3 = driver_laps['Sector3Time'].dt.total_seconds().dropna()

        throttle_vals, brake_vals, speed_vals = [], [], []
        for _, lap in driver_laps.iterlaps():
            try:
                tel = lap.get_car_data()
                if tel.empty:
                    continue
                throttle_vals.append(tel['Throttle'].mean())
                brake_vals.append(tel['Brake'].mean())
                speed_vals.append(tel['Speed'].max())
            except Exception:
                continue

        rows.append({
            'season':          year,
            'round':           round_num,
            'session_type':    session_type,
            'driver_id':       driver,
            'team':            driver_laps['Team'].iloc[0],
            'best_lap_s':      lap_times.min(),
            'mean_lap_s':      lap_times.mean(),
            'lap_consistency': lap_times.std(),
            'mean_s1':         s1.mean(),
            'mean_s2':         s2.mean(),
            'mean_s3':         s3.mean(),
            'mean_throttle':   pd.Series(throttle_vals).mean(),
            'mean_brake':      pd.Series(brake_vals).mean(),
            'max_speed':       pd.Series(speed_vals).max(),
            'lap_count':       len(driver_laps),
        })

    return pd.DataFrame(rows)


# ── main ──────────────────────────────────────────────────────────────────────
import os
os.makedirs('f1_data', exist_ok=True)

all_features, done_sessions = load_checkpoint()
all_features = [all_features] if not all_features.empty else []

for year in tqdm(YEARS, desc='Seasons'):

    schedule = get_schedule_safe(year)
    if schedule is None:
        continue  # skip year entirely if schedule is unavailable

    for _, event in tqdm(schedule.iterrows(),
                         total=len(schedule),
                         desc=f'  {year} rounds',
                         leave=False):
        round_num = event['RoundNumber']

        for stype in SESSION_TYPES:

            # ── skip if already extracted (checkpoint)
            if (year, round_num, stype) in done_sessions:
                continue

            df = extract_session_features(year, round_num, stype)

            if not df.empty:
                all_features.append(df)
                done_sessions.add((year, round_num, stype))

                # ── save incrementally so a crash doesn't lose progress
                combined = pd.concat(all_features, ignore_index=True)
                combined.to_csv(CHECKPOINT_FILE, index=False, encoding='utf-8-sig')

            time.sleep(DELAY)

print(f"\n✅ Done — {CHECKPOINT_FILE}")