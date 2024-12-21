import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL database configuration
db_config = {
    'host': 'localhost',        # Change to your MySQL host
    'user': 'root',             # Change to your MySQL username
    'password': '', # Change to your MySQL password
    'database': 'steam_games_info',  # Change to your database name
}

def create_connection():
    """Create and return a MySQL connection."""
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print("Connected to MySQL database")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None

def import_csv_to_mysql(csv_file, table_name, columns):
    """Import a CSV file into a specified MySQL table."""
    # Connect to MySQL
    conn = create_connection()
    if not conn:
        return

    cursor = conn.cursor()

    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file, encoding='utf-8', engine='python', delimiter=',', quotechar='"', quoting=1)

    # Replace NaN with None for MySQL NULL
    df = df.where(pd.notnull(df), None)

    # Insert DataFrame rows into the MySQL table
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'`{col}`' for col in columns])  # Enclose columns in backticks
        sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
        values = tuple(row[col] for col in columns)
        try:
            cursor.execute(sql, values)
        except Error as e:
            print(f"Error inserting row: {e}")

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data imported successfully into {table_name}")


# Import data into each table
# import_csv_to_mysql('Games_cleaned.csv', 'Games', ['AppID', 'Name', 'ReleaseDate', 'AboutTheGame', 'Website', 'HeaderImage'])
# done import_csv_to_mysql('Publishers.csv', 'Publishers', ['AppID', 'Publisher'])
# done import_csv_to_mysql('Developers.csv', 'Developers', ['AppID', 'Developer'])
# done import_csv_to_mysql('Categories.csv', 'Categories', ['CategoryID', 'Categories'])
# done import_csv_to_mysql('Game_Categories.csv', 'Game_Categories', ['AppID', 'CategoryID'])
# done import_csv_to_mysql('User_Metrics.csv', 'User_Metrics', ['AppID', 'EstimatedOwners', 'PeakCCU', 'UserScore', 'Positive', 'Negative', 'ScoreRank', 'Achievements', 'Recommendations'])
# done import_csv_to_mysql('System_Requirements.csv', 'System_Requirements', ['AppID', 'Windows', 'Mac', 'Linux'])
# import_csv_to_mysql('Price_History.csv', 'Price_History', ['AppID', 'Price', 'Discount'])
