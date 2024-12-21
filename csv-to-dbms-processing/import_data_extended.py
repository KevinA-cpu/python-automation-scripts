import pandas as pd
from sqlalchemy import create_engine, text

# Database connection (replace with your credentials)
DB_USER = 'root'
DB_PASS = ''
DB_HOST = 'localhost'
DB_NAME = 'steam_games_info'
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')


# Load the CSV file
data_file = 'games.csv'
df = pd.read_csv(data_file,encoding='utf-8', engine='python')

# Helper function to save DataFrame to MySQL
def save_to_db(df, table_name, if_exists='append'):
    try:
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        print(f"Data saved to table: {table_name}")
    except Exception as e:
        print(f"Error saving to {table_name}: {e}")

def query_db(sql):
    with engine.connect() as connection:
        result = connection.execute(text(sql))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def clean_language_array(text):
    if pd.isna(text):
        return []
    try:
        # Remove brackets and split by comma-space WITH quotes
        cleaned = text.strip('[]').split("', '")
        # Clean up the first and last elements
        if cleaned:
            cleaned[0] = cleaned[0].strip("'")
            cleaned[-1] = cleaned[-1].strip("'")
        return cleaned
    except:
        print(f"Error processing: {text}")
        return []

def clean_reviews(text):
    if pd.isna(text):
        return []
    try:
        # Split by double whitespace
        reviews = text.split('  ')
        # Remove empty strings and strip whitespace
        return [review.strip() for review in reviews if review.strip()]
    except:
        return []
# 1. Games table
# games_df = pd.read_csv('Games_cleaned.csv', encoding='utf-8', engine='python')
# games = games_df[['AppID', 'Name', 'ReleaseDate', 'AboutTheGame', 'Website', 'HeaderImage']]
# games.columns = ['AppID', 'Name', 'ReleaseDate', 'AboutTheGame', 'Website', 'HeaderImage']
# save_to_db(games, 'games')

# 2. Price history
# price_history = df[['AppID', 'Price', 'Discount']]
# save_to_db(price_history, 'price_history')

# 3. System requirements

# system_requirements = df[['AppID', 'Windows', 'Mac', 'Linux']]
# system_requirements[['Windows', 'Mac', 'Linux']] = system_requirements[['Windows', 'Mac', 'Linux']].astype(bool)
# save_to_db(system_requirements, 'system_requirements')

# 4. User metrics
# user_metrics = df[['AppID', 'Estimated owners', 'Peak CCU', 'User score', 'Positive', 'Negative',
#                    'Score rank', 'Achievements', 'Recommendations', 'Average playtime forever',
#                    'Average playtime two weeks', 'Median playtime forever', 'Median playtime two weeks']]
# user_metrics.columns = ['AppID', 'EstimatedOwners', 'PeakCCU', 'UserScore', 'Positive', 'Negative', 'ScoreRank',
#                         'Achievements', 'Recommendations', 'AveragePlaytimeForever', 'AveragePlaytimeTwoWeeks',
#                         'MedianPlaytimeForever', 'MedianPlaytimeTwoWeeks']
# save_to_db(user_metrics, 'user_metrics')

# 5. Developers
# developers = df[['AppID', 'Developers']]
# developers['Developers'] = developers['Developers'].str.split(',')
# developers = developers.explode('Developers').drop_duplicates()
# developers.rename(columns={'Developers': 'Developer'}, inplace=True)
# save_to_db(developers, 'developers')

# 6. Publishers
# publishers = df[['AppID', 'Publishers']]
# publishers['Publishers'] = publishers['Publishers'].str.split(',')
# publishers = publishers.explode('Publishers').drop_duplicates()
# publishers.rename(columns={'Publishers': 'Publisher'}, inplace=True)
# save_to_db(publishers, 'publishers')

# 7. Categories
# categories = df[['Categories']].drop_duplicates()
# categories['Categories'] = categories['Categories'].str.split(',')
# categories = categories.explode('Categories').drop_duplicates().dropna()
# categories.rename(columns={'Categories': 'Category'}, inplace=True)
# save_to_db(categories, 'categories')

# 8. Game categories (many-to-many)
# categories_with_ids = query_db("SELECT ID as CategoryID, Category FROM categories")
# game_categories = df[['AppID', 'Categories']].drop_duplicates()
# game_categories['Categories'] = game_categories['Categories'].str.split(',')
# game_categories = game_categories.explode('Categories').dropna()
# game_categories = game_categories.merge(categories_with_ids, left_on='Categories', right_on='Category')
# game_categories = game_categories[['AppID', 'CategoryID']]
# game_categories = game_categories.drop_duplicates()
# save_to_db(game_categories, 'game_categories')

# 9. Genres
# genres = df[['Genres']].drop_duplicates()
# genres['Genres'] = genres['Genres'].str.split(',')
# genres = genres.explode('Genres').drop_duplicates().dropna()
# genres.rename(columns={'Genres': 'Genre'}, inplace=True)
# save_to_db(genres, 'genres')

# 10. Game genres (many-to-many)
# genres_with_ids = query_db("SELECT GenreID, Genre FROM genres")
# game_genres = df[['AppID', 'Genres']].drop_duplicates()
# game_genres['Genres'] = game_genres['Genres'].str.split(',')
# game_genres = game_genres.explode('Genres').dropna()
# game_genres = game_genres.merge(genres_with_ids, left_on='Genres', right_on='Genre')
# game_genres = game_genres[['AppID', 'GenreID']]
# save_to_db(game_genres, 'game_genres')


# 11. Screenshots
# screenshots = df[['AppID', 'Screenshots']]
# screenshots['Screenshots'] = screenshots['Screenshots'].str.split(',')
# screenshots = screenshots.explode('Screenshots').drop_duplicates()
# screenshots.rename(columns={'Screenshots': 'ScreenshotURL'}, inplace=True)
# save_to_db(screenshots, 'screenshots')

# 12. Movies
# movies = df[['AppID', 'Movies']]
# movies['Movies'] = movies['Movies'].str.split(',')
# movies = movies.explode('Movies').drop_duplicates()
# movies.rename(columns={'Movies': 'MovieURL'}, inplace=True)
# save_to_db(movies, 'movies')

# 13. Supported languages

# supported_languages = df[['AppID', 'Supported languages']]
# supported_languages['Supported languages'] = supported_languages['Supported languages'].apply(clean_language_array)
# supported_languages = supported_languages.explode('Supported languages').drop_duplicates()
# supported_languages.rename(columns={'Supported languages': 'Language'}, inplace=True)
# supported_languages = supported_languages.dropna()
# save_to_db(supported_languages, 'supported_languages')

# 14. Full audio languages
# full_audio_languages = df[['AppID', 'Full audio languages']]
# full_audio_languages['Full audio languages'] = full_audio_languages['Full audio languages'].apply(clean_language_array)
# full_audio_languages = full_audio_languages.explode('Full audio languages').drop_duplicates()
# full_audio_languages.rename(columns={'Full audio languages': 'Language'}, inplace=True)
# full_audio_languages = full_audio_languages.dropna()    
# save_to_db(full_audio_languages, 'full_audio_languages')

# 15. Tags
# tags = df[['Tags']].drop_duplicates()
# tags['Tags'] = tags['Tags'].str.split(',')
# tags = tags.explode('Tags').drop_duplicates().dropna()
# tags.rename(columns={'Tags': 'Tag'}, inplace=True)
# save_to_db(tags, 'tags')

# 16. Game tags (many-to-many)
# tags_with_ids = query_db("SELECT TagID, Tag FROM tags")
# game_tags = df[['AppID', 'Tags']].drop_duplicates()
# game_tags['Tags'] = game_tags['Tags'].str.split(',')
# game_tags = game_tags.explode('Tags').dropna()
# game_tags = game_tags.merge(tags_with_ids, left_on='Tags', right_on='Tag')
# game_tags = game_tags[['AppID', 'TagID']]
# save_to_db(game_tags, 'game_tags')

# 17. Reviews
# reviews = df[['AppID', 'Reviews']]
# reviews['Reviews'] = reviews['Reviews'].apply(clean_reviews)
# reviews = reviews.explode('Reviews').drop_duplicates()
# reviews.rename(columns={'Reviews': 'Review'}, inplace=True)
# reviews = reviews.dropna()
# save_to_db(reviews, 'reviews')

print("All data processed and saved.")
