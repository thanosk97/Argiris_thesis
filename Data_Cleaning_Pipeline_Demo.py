import ast
import numpy as np
import pandas as pd
from pathlib import Path

DATA_DIR = Path("f1_data")
OUT_DIR = Path("f1_data/cleaned")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_time_to_seconds(x):
    """Parse strings like '1:29.374', '32:04.660', '+22.457' into seconds when possible."""
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if not s:
        return np.nan
    if s.startswith("+"):  # gap format, not absolute lap time
        s = s[1:]
    parts = s.split(":")
    try:
        if len(parts) == 1:
            return float(parts[0])
        if len(parts) == 2:
            m, sec = parts
            return int(m) * 60 + float(sec)
        if len(parts) == 3:
            h, m, sec = parts
            return int(h) * 3600 + int(m) * 60 + float(sec)
    except Exception:
        return np.nan
    return np.nan

def load():
    return {
        "results": pd.read_csv(DATA_DIR / "Results.csv"),
        "quali": pd.read_csv(DATA_DIR / "Qualifying.csv"),
        "sprint": pd.read_csv(DATA_DIR / "Sprint.csv"),
        "ds": pd.read_csv(DATA_DIR / "DriverStandings.csv"),
        "cs": pd.read_csv(DATA_DIR / "ConstructorStandings.csv"),
        "pit": pd.read_csv(DATA_DIR / "PitStops.csv"),
        "laps": pd.read_csv(DATA_DIR / "Laps.csv"),
        "drivers": pd.read_csv(DATA_DIR / "Drivers.csv"),
        "ff1": pd.read_csv(DATA_DIR / "FF1_Features.csv"),
    }

def clean_results(df):
    out = df.copy()
    num_cols = ["position", "grid", "points", "laps", "time_millis", "FastestLap_rank", "FastestLap_lap"]
    for c in num_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["race_date"] = pd.to_datetime(out["date"], errors="coerce")
    out["fastest_lap_s"] = out["FastestLap_time_time"].map(parse_time_to_seconds)
    out["finish_time_s"] = out["time_time"].map(parse_time_to_seconds)
    out["is_classified"] = out["positionText"].astype(str).str.match(r"^\d+$", na=False).astype(int)
    out["is_dnf"] = (out["status"].astype(str).str.contains("Retired|Accident|Disqualified|DNF", case=False, na=False)).astype(int)
    return out

def clean_quali(df):
    out = df.copy()
    out["Q1_s"] = out["Q1"].map(parse_time_to_seconds)
    out["Q2_s"] = out["Q2"].map(parse_time_to_seconds)
    out["Q3_s"] = out["Q3"].map(parse_time_to_seconds)
    out["quali_position"] = pd.to_numeric(out["position"], errors="coerce")
    keep = ["season","round","driver_driverId","quali_position","Q1_s","Q2_s","Q3_s"]
    return out[keep]

def clean_pitstops(df):
    out = df.copy()
    out["pit_lap"] = pd.to_numeric(out["lap"], errors="coerce")
    out["pit_duration_s"] = pd.to_numeric(out["duration"], errors="coerce")
    agg = out.groupby(["season","round","driverId"], as_index=False).agg(
        pitstop_count=("stop", "count"),
        pit_duration_total_s=("pit_duration_s", "sum"),
        pit_duration_mean_s=("pit_duration_s", "mean"),
        first_pit_lap=("pit_lap", "min"),
    )
    agg = agg.rename(columns={"driverId": "driver_driverId"})
    return agg

def clean_driver_standings(df):
    out = df.copy()
    out["points"] = pd.to_numeric(out["points"], errors="coerce")
    out["wins"] = pd.to_numeric(out["wins"], errors="coerce")
    out["position"] = pd.to_numeric(out["position"], errors="coerce")
    out = out.sort_values(["driver_driverId", "season", "round"])
    out["driver_points_pre"] = out.groupby(["driver_driverId","season"])["points"].shift(1).fillna(0)
    out["driver_wins_pre"] = out.groupby(["driver_driverId","season"])["wins"].shift(1).fillna(0)
    out["driver_rank_pre"] = out.groupby(["driver_driverId","season"])["position"].shift(1).fillna(99)
    return out[["season","round","driver_driverId","driver_points_pre","driver_wins_pre","driver_rank_pre"]]

def clean_constructor_standings(df):
    out = df.copy()
    out["points"] = pd.to_numeric(out["points"], errors="coerce")
    out["wins"] = pd.to_numeric(out["wins"], errors="coerce")
    out["position"] = pd.to_numeric(out["position"], errors="coerce")
    out = out.sort_values(["constructor_constructorId", "season", "round"])
    out["constructor_points_pre"] = out.groupby(["constructor_constructorId","season"])["points"].shift(1).fillna(0)
    out["constructor_rank_pre"] = out.groupby(["constructor_constructorId","season"])["position"].shift(1).fillna(99)
    return out[["season","round","constructor_constructorId","constructor_points_pre","constructor_rank_pre"]]

def clean_ff1(ff1, drivers):
    out = ff1.copy()
    # FastF1 'driver_id' looks like code (e.g., VER). Map to Jolpica driverId.
    code_to_driver = drivers.dropna(subset=["code"]).set_index("code")["driverId"].to_dict()
    out["driver_driverId"] = out["driver_id"].map(code_to_driver)

    # Keep only rows we can map.
    out = out.dropna(subset=["driver_driverId"])

    feat_cols = ["best_lap_s","mean_lap_s","lap_consistency","mean_s1","mean_s2","mean_s3","mean_throttle","mean_brake","max_speed","lap_count"]
    wide = out.pivot_table(
        index=["season","round","driver_driverId"],
        columns="session_type",
        values=feat_cols,
        aggfunc="mean"
    )
    wide.columns = [f"{sess}_{feat}" for feat, sess in wide.columns]
    wide = wide.reset_index()
    return wide

def build_training_table(raw):
    results = clean_results(raw["results"])
    quali = clean_quali(raw["quali"])
    pit = clean_pitstops(raw["pit"])
    ds = clean_driver_standings(raw["ds"])
    cs = clean_constructor_standings(raw["cs"])
    ff1 = clean_ff1(raw["ff1"], raw["drivers"])

    model = (results
        .merge(quali, on=["season","round","driver_driverId"], how="left")
        .merge(pit, on=["season","round","driver_driverId"], how="left")
        .merge(ds, on=["season","round","driver_driverId"], how="left")
        .merge(cs, on=["season","round","constructor_constructorId"], how="left")
        .merge(ff1, on=["season","round","driver_driverId"], how="left")
    )

    # Example targets
    model["target_finish_pos"] = model["position"]  # regression or ordinal
    model["target_podium"] = (model["position"] <= 3).astype(float)
    model["target_fastest_lap"] = (model["FastestLap_rank"] == 1).astype(float)

    return model

def main():
    raw = load()
    model = build_training_table(raw)

    # Basic sanity filters for modeling table
    model = model.drop_duplicates(subset=["season","round","driver_driverId"])
    model = model.sort_values(["season","round","position"], na_position="last")

    model.to_csv(OUT_DIR / "model_table.csv", index=False)
    print(f"Saved {len(model)} rows to {OUT_DIR / 'model_table.csv'}")

if __name__ == "__main__":
    main()
