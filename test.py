# Import necessary libraries
import pandas as pd

# Load the dataset (adjust the path to where your file is located)
constructor_results = pd.read_csv('constructor_results.csv')

# Display the first few rows of the dataset
print(constructor_results.head())

# Get an overview of the dataset
print(constructor_results.info())

# Summary statistics
print(constructor_results.describe())

# Check for missing values
print(constructor_results.isnull().sum())

# Handle missing values (example: fill with 0 or drop)
constructor_results.fillna(0, inplace=True)  # Example: Fill NaN with 0
# OR
constructor_results.dropna(inplace=True)    # Drop rows with missing values

import matplotlib.pyplot as plt
import seaborn as sns

# Example: Distribution of points scored by constructors
sns.histplot(constructor_results['points'], kde=True, bins=30)
plt.title('Distribution of Constructor Points')
plt.xlabel('Points')
plt.ylabel('Frequency')
plt.show()

# Example: Count of unique constructors
print(constructor_results['constructorId'].value_counts())

# Example: Constructors with the most wins
top_constructors = constructor_results.groupby('constructorId')['points'].sum().sort_values(ascending=False)
print(top_constructors.head(10))

# Visualize top constructors' performance
top_constructors.head(10).plot(kind='bar', color='skyblue')
plt.title('Top Constructors by Total Points')
plt.ylabel('Total Points')
plt.xlabel('Constructor ID')
plt.show()


# Load additional datasets
races = pd.read_csv('races.csv')
drivers = pd.read_csv('drivers.csv')

# Merge datasets (example: merging races with constructor results)
combined_data = pd.merge(constructor_results, races, on='raceId', how='left')

# Merge with driver data (if needed)
combined_data = pd.merge(combined_data, drivers, on='driverId', how='left')

# Inspect the combined dataset
print(combined_data.head())


# Create new features
combined_data['constructor_efficiency'] = combined_data['points'] / combined_data['laps']  # Points per lap
combined_data['is_winner'] = combined_data['positionOrder'].apply(lambda x: 1 if x == 1 else 0)  # Binary winner column

# Example: Calculate season totals
season_totals = combined_data.groupby(['year', 'constructorId'])['points'].sum().reset_index()
print(season_totals.head())

# Example: Create a cumulative points column
combined_data['cumulative_points'] = combined_data.groupby(['constructorId'])['points'].cumsum()

# Sort data by date
combined_data['date'] = pd.to_datetime(combined_data['date'])
combined_data = combined_data.sort_values(by='date')

# Resample data (e.g., aggregate points by year)
annual_points = combined_data.groupby(['year', 'constructorId'])['points'].sum().reset_index()
print(annual_points.head())

# Plot performance over time
for constructor in top_constructors.index[:5]:  # Top 5 constructors
    subset = annual_points[annual_points['constructorId'] == constructor]
    plt.plot(subset['year'], subset['points'], label=f'Constructor {constructor}')
plt.title('Constructor Performance Over Time')
plt.xlabel('Year')
plt.ylabel('Points')
plt.legend()
plt.show()

# Save the prepared dataset to a CSV file
combined_data.to_csv('prepared_formula1_data.csv', index=False)
