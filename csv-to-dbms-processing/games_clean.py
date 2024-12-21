import pandas as pd

# Load the data
games = pd.read_csv('Games.csv')

# Convert 'Release date' to datetime format, handling errors
games['Release date'] = pd.to_datetime(games['Release date'], format='%d-%b-%y', errors='coerce')

# Format the date to 'YYYY-MM-DD'
games['Release date'] = games['Release date'].dt.strftime('%Y-%m-%d')

# Save the cleaned data
games.to_csv('Games_cleaned.csv', index=False)

print("Games_cleaned.csv has been saved with MySQL-compatible release dates.")
