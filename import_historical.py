import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

# Database Configuration
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '5432')

# Create database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def import_historical_data():
    try:
        # Read the CSV file
        csv_path = "/app/crosswordimport.csv"
        logging.info(f"Reading CSV from {csv_path}")
        historical_df = pd.read_csv(csv_path)
        logging.info(f"Successfully read {len(historical_df)} rows from CSV")

        # Upload to database
        logging.info("Attempting to write to database")
        historical_df.to_sql(
            'crossword_stats',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logging.info("Successfully imported historical data")
    except Exception as e:
        logging.error(f"Error during import: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        import_historical_data()
    except Exception as e:
        logging.error(f"Error during import: {str(e)}")
        sys.exit(1)
