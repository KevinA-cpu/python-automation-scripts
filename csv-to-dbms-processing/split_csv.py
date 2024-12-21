import pandas as pd

# Load the CSV
df = pd.read_csv('games_data.csv' , encoding='utf-8', engine='python')

# 1. Games Table
games = df[['AppID', 'Name', 'Release date', 'About the game', 'Website', 'Header image']].drop_duplicates()
games.to_csv('Games.csv', index=False)

# 2. Publishers Table
publishers = df[['AppID', 'Publishers']].drop_duplicates()
publishers['Publishers'] = publishers['Publishers'].str.split(',')
publishers = publishers.explode('Publishers')
publishers['Publishers'] = publishers['Publishers'].str.strip()
publishers = publishers.drop_duplicates()
publishers.to_csv('Publishers.csv', index=False)

# 3. Developers Table
developers = df[['AppID', 'Developers']].drop_duplicates()
developers['Developers'] = developers['Developers'].str.split(',')
developers = developers.explode('Developers')
developers['Developers'] = developers['Developers'].str.strip()
developers = developers.drop_duplicates()
developers.to_csv('Developers.csv', index=False)

# 4. Categories Table
categories = df[['Categories']].drop_duplicates()
categories['Categories'] = categories['Categories'].str.split(',')
categories = categories.explode('Categories')
categories['Categories'] = categories['Categories'].str.strip()
categories = categories.drop_duplicates().dropna()
categories['CategoryID'] = range(1, len(categories) + 1)
categories.to_csv('Categories.csv', index=False)

# 5. Game_Categories Table (Many-to-Many)
game_categories = df[['AppID', 'Categories']].drop_duplicates()
game_categories['Categories'] = game_categories['Categories'].str.split(',')
game_categories = game_categories.explode('Categories')
game_categories['Categories'] = game_categories['Categories'].str.strip()
game_categories = game_categories.dropna()
game_categories = game_categories.merge(categories, on='Categories', how='left')
game_categories = game_categories[['AppID', 'CategoryID']].drop_duplicates()
game_categories.to_csv('Game_Categories.csv', index=False)

# 6. User_Metrics Table
user_metrics = df[['AppID', 'Estimated owners', 'Peak CCU', 'User score', 'Positive', 'Negative', 'Score rank', 'Achievements', 'Recommendations']].drop_duplicates()
user_metrics.to_csv('User_Metrics.csv', index=False)

# 7. System_Requirements Table
system_requirements = df[['AppID', 'Windows', 'Mac', 'Linux']].drop_duplicates()
system_requirements.to_csv('System_Requirements.csv', index=False)

# 8. Price_History Table
price_history = df[['AppID', 'Price', 'Discount']].drop_duplicates()
price_history.to_csv('Price_History.csv', index=False)

print("Data has been split into multiple CSV files.")
