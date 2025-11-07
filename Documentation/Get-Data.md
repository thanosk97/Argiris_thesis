# Get-Data.py Documentation

## Overview

`Get-Data.py` is a Python script that fetches comprehensive Formula 1 historical data from the Ergast API and exports it to CSV files. The script handles pagination, rate limiting, retries, and data normalization automatically.

## Features

- **Comprehensive Data Coverage**: Retrieves seasons, drivers, constructors, circuits, race results, qualifying, sprint races, standings, pit stops, and lap times
- **Automatic Pagination**: Handles large datasets by fetching data in batches
- **Rate Limiting**: Implements exponential backoff for HTTP 429 responses
- **Retry Logic**: Automatically retries failed requests
- **Progress Tracking**: Shows progress bars for multi-season data fetching
- **Data Normalization**: Flattens nested JSON structures and standardizes column names

## Configuration

| Constant | Default Value | Description |
|----------|---------------|-------------|
| `BASE_URL` | `https://api.jolpi.ca/ergast/f1` | Ergast API endpoint |
| `OUTPUT_DIR` | `f1_data/` | Directory for CSV output |
| `BATCH_SIZE` | 1000 | Records per API request |
| `REQUEST_TIMEOUT` | 60 | Request timeout in seconds |
| `RETRIES` | 5 | Number of retry attempts |
| `DELAY` | 2 | Delay between requests in seconds |

## Dependencies

```python
import requests
import pandas as pd
import os
import time
from tqdm import tqdm
```

## Core Functions

### Utilities

#### `safe_get_json(url, retries=RETRIES)`

Performs HTTP GET requests with retry logic and exponential backoff for rate limiting.

**Parameters:**
- `url` (str): API endpoint URL
- `retries` (int): Number of retry attempts (default: 5)

**Returns:** JSON response or `None` on failure

**Features:**
- Handles HTTP 429 (Too Many Requests) with exponential backoff
- Implements retry logic for failed requests
- Includes timeout handling

#### `flatten(df)`

Normalizes DataFrame column names by replacing dots with underscores and standardizing naming conventions.

**Parameters:**
- `df` (DataFrame): Input DataFrame

**Returns:** Flattened DataFrame with standardized column names

**Transformations:**
- `Driver.` → `driver_`
- `Constructor.` → `constructor_`
- `Circuit.` → `circuit_`
- `Time.` → `time_`
- `AverageSpeed.` → `avgSpeed_`

#### `save_csv(df, name)`

Saves DataFrame to CSV file in the output directory.

**Parameters:**
- `df` (DataFrame): DataFrame to save
- `name` (str): Output filename (without extension)

**Features:**
- Skips empty DataFrames
- Uses UTF-8 with BOM encoding
- Displays save confirmation with row count

### Data Fetching Functions

#### `fetch_paginated(url, key_path)`

Retrieves all data from paginated API endpoints.

**Parameters:**
- `url` (str): Base API endpoint URL
- `key_path` (list): Nested JSON keys to extract data (e.g., `["SeasonTable", "Seasons"]`)

**Returns:** Flattened DataFrame with all records

**Process:**
1. Fetches data in batches of `BATCH_SIZE`
2. Handles pagination using offset parameter
3. Continues until no more data is available
4. Normalizes and flattens the result

#### Basic Data Endpoints

##### `fetch_all_seasons()`
Retrieves all F1 seasons from the API.

**Returns:** DataFrame containing season data

##### `fetch_all_drivers()`
Retrieves all F1 drivers from the API.

**Returns:** DataFrame containing driver information

##### `fetch_all_constructors()`
Retrieves all F1 constructors (teams) from the API.

**Returns:** DataFrame containing constructor information

##### `fetch_all_circuits()`
Retrieves all F1 circuits from the API.

**Returns:** DataFrame containing circuit information

### Season-Specific Functions

#### `get_races_for_season(year)`

Retrieves the race schedule for a specific season.

**Parameters:**
- `year` (int): Season year

**Returns:** List of race objects containing race metadata

#### `fetch_race_data(year, dataset)`

Fetches race-specific data for all rounds in a season.

**Parameters:**
- `year` (int): Season year
- `dataset` (str): Data type to fetch (`"results"`, `"qualifying"`, `"sprint"`, `"pitstops"`, `"laps"`)

**Returns:** DataFrame with race metadata and requested data

**Race Metadata Included:**
- `season`: Season year
- `round`: Round number
- `raceName`: Race name
- `date`: Race date
- `circuit_id`: Circuit identifier
- `circuit_name`: Circuit name
- `circuit_location`: Circuit city
- `circuit_country`: Circuit country

#### `fetch_standings_per_round(year, dataset)`

Retrieves driver or constructor standings after each race round.

**Parameters:**
- `year` (int): Season year
- `dataset` (str): `"driverStandings"` or `"constructorStandings"`

**Returns:** DataFrame with standings data per round

#### `fetch_all_years(dataset, start_year=2024, end_year=2024)`

Aggregates data across multiple seasons with progress tracking.

**Parameters:**
- `dataset` (str): Data type to fetch
- `start_year` (int): Starting season (default: 2024)
- `end_year` (int): Ending season (default: 2024)

**Returns:** Combined DataFrame for all seasons

**Features:**
- Progress bar display using tqdm
- Handles both race data and standings
- Combines data from all seasons into single DataFrame

## Main Pipeline

### `main()`

Orchestrates the entire data collection process.

**Process:**
1. Fetches reference data (seasons, drivers, constructors, circuits)
2. Fetches race-specific data for configured year range
3. Fetches standings data per round
4. Exports all datasets to CSV files
5. Displays completion status and output location

**Datasets Collected:**
- Seasons
- Drivers
- Constructors
- Circuits
- Results
- Qualifying
- Sprint
- Driver Standings
- Constructor Standings
- Pit Stops
- Laps

## Output Files

All CSV files are saved in the `f1_data/` directory:

| File | Description |
|------|-------------|
| `Seasons.csv` | All F1 seasons |
| `Drivers.csv` | All F1 drivers |
| `Constructors.csv` | All F1 constructors/teams |
| `Circuits.csv` | All F1 circuits |
| `Results.csv` | Race results |
| `Qualifying.csv` | Qualifying session results |
| `Sprint.csv` | Sprint race results |
| `DriverStandings.csv` | Driver championship standings per round |
| `ConstructorStandings.csv` | Constructor championship standings per round |
| `PitStops.csv` | Pit stop data |
| `Laps.csv` | Lap-by-lap timing data |

## Usage

### Basic Usage

```bash
python Get-Data.py
```

### Fetching Data for Different Years

Modify the `fetch_all_years()` calls in the `main()` function:

```python
datasets = {
    # ... other datasets ...
    "Results": fetch_all_years("results", start_year=2020, end_year=2024),
    "Qualifying": fetch_all_years("qualifying", start_year=2020, end_year=2024),
    # ... other datasets ...
}
```

### Example: Fetch Last 5 Seasons

```python
current_year = 2024
start_year = current_year - 4

datasets = {
    "Results": fetch_all_years("results", start_year=start_year, end_year=current_year),
    # ... other datasets ...
}
```

## Error Handling

The script implements robust error handling:

- **Automatic Retries**: Failed requests are retried up to 5 times
- **Exponential Backoff**: HTTP 429 responses trigger exponential backoff strategy
- **Graceful Degradation**: Missing data is skipped with warnings
- **Timeout Protection**: Requests timeout after 60 seconds
- **Console Warnings**: Displays warnings for skipped datasets and errors

## Performance Considerations

- **Rate Limiting**: 2-second delay between requests to avoid overwhelming the API
- **Batch Processing**: Fetches data in batches of 1000 records
- **Progress Tracking**: Visual progress bars for multi-year data fetching
- **Efficient Concatenation**: Uses pandas concat for combining DataFrames

## API Reference

The script uses the Ergast API endpoints:

- Base URL: `https://api.jolpi.ca/ergast/f1`
- Supports pagination via `limit` and `offset` parameters
- Returns JSON formatted data
- Rate limited (handled automatically by the script)

## Troubleshooting

### No Data Retrieved

- Check internet connection
- Verify API endpoint is accessible
- Increase `RETRIES` and `DELAY` values

### HTTP 429 Errors

- Increase `DELAY` between requests
- The script handles this automatically with exponential backoff

### Timeout Errors

- Increase `REQUEST_TIMEOUT` value
- Check network stability

### Empty DataFrames

- Verify the year range includes valid F1 seasons
- Check if the specific dataset exists for those years (e.g., sprint races only exist from 2021)

## Notes

- Sprint race data is only available from the 2021 season onwards
- Some historical data may be incomplete for older seasons
- The script creates the output directory automatically if it doesn't exist
- CSV files use UTF-8 with BOM encoding for better Excel compatibility
