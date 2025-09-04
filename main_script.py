import pandas as pd
import sqlite3
import logging
import os
import time
from datetime import datetime
import schedule

# -----------------------------
# Logging setup
# -----------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, "flow.log")

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load CSV file into a pandas DataFrame.
    Raises an error if file not found or invalid.
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded CSV file successfully: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading CSV file {file_path}: {e}")
        raise

def create_database_if_not_exists(db_path: str):
    """
    Ensure the database file exists. If not, create it.
    """
    db_folder = os.path.dirname(db_path)
    os.makedirs(db_folder, exist_ok=True)

    if not os.path.exists(db_path):
        # Creating an empty database file
        conn = sqlite3.connect(db_path)
        conn.close()
        logging.info(f"Created new SQLite database: {db_path}")
    else:
        logging.info(f"Database already exists: {db_path}")

def write_to_sql(df: pd.DataFrame, db_name: str, table_name: str):
    """
    Write DataFrame to SQL table (SQLite used here).
    Overwrites the table if it already exists.
    """
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"Data successfully written to {db_name} -> {table_name}")
    except Exception as e:
        logging.error(f"Error writing to SQL: {e}")
        raise

def main():
    """
    Main execution flow: read CSV and update SQL table.
    """
    logging.info("=== Script started ===")

    # Config
    csv_file = "data/FactTransactions.csv"
    db_file = "data/my_database.db"
    table_name = "my_table"

    try:
        # Ensure DB file and folder exist
        create_database_if_not_exists(db_file)

        # Load data and write to DB
        df = load_csv(csv_file)
        write_to_sql(df, db_file, table_name)
        logging.info("=== Script finished successfully ===")
    except Exception as e:
        logging.error(f"Script failed: {e}")

if __name__ == "__main__":
    main()
