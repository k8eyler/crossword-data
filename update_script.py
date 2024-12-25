import os
import pandas as pd
from datetime import datetime, timedelta
import subprocess
from pathlib import Path
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import argparse

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/data/crossword_updates.log'),
        logging.StreamHandler()
    ]
)

def parse_args():
    parser = argparse.ArgumentParser(description='Update crossword stats with flexible date range')
    parser.add_argument(
        '--days-back',
        type=int,
        default=30,
        help='Number of days to look back (default: 30)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format (overrides days-back if provided)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date in YYYY-MM-DD format (default: today)'
    )
    return parser.parse_args()

def upsert_crossword_stats(df, engine):
    """
    Upsert crossword stats and track solving sessions.
    """
    # Clean the DataFrame
    df = df.copy()
    current_date = datetime.now().date()
    
    # Type conversions
    type_conversions = {
        'puzzle_id': 'int64',  # Use int64 for bigint compatibility
        'day_of_week_integer': 'int32',
        'version': 'int32',
        'percent_filled': 'float64',
        'solved': 'bool',
        'solving_seconds': 'Int64'  # Nullable integer type
    }
    
    # Apply type conversions
    for col, dtype in type_conversions.items():
        if col in df.columns:
            try:
                if dtype == 'Int64':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logging.error(f"Error converting column {col} to {dtype}: {str(e)}")
    
    # Replace NaN/None with proper SQL NULL
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    
    # Convert DataFrame to records
    records = df.to_dict('records')
    updates = 0
    inserts = 0
    sessions_added = 0
    errors = 0
    
    with engine.connect() as connection:
        for record in records:
            try:
                puzzle_id = record['puzzle_id']
                current_solving_seconds = record.get('solving_seconds', 0)
                
                # Get previous total solving time from sessions
                previous_total_query = text("""
                    SELECT COALESCE(SUM(solving_seconds), 0) as total_seconds
                    FROM puzzle_sessions
                    WHERE puzzle_id = :puzzle_id
                """)
                previous_total = connection.execute(
                    previous_total_query, 
                    {"puzzle_id": puzzle_id}
                ).scalar() or 0
                
                # Calculate new session time (if any)
                session_seconds = max(0, current_solving_seconds - previous_total) if current_solving_seconds else 0
                
                # If there's new solving time, add a session
                if session_seconds > 0:
                    session_insert = text("""
                        INSERT INTO puzzle_sessions 
                        (puzzle_id, session_date, solving_seconds)
                        VALUES (:puzzle_id, :session_date, :solving_seconds)
                    """)
                    connection.execute(session_insert, {
                        "puzzle_id": puzzle_id,
                        "session_date": current_date,
                        "solving_seconds": session_seconds
                    })
                    sessions_added += 1
                    logging.info(f"Added new session for puzzle_id {puzzle_id}: {session_seconds} seconds")
                
                # Check if puzzle exists in stats table
                exists = connection.execute(
                    text("SELECT 1 FROM crossword_stats WHERE puzzle_id = :puzzle_id"),
                    {"puzzle_id": puzzle_id}
                ).first() is not None

                if exists:
                    # Update existing puzzle stats
                    update_stmt = text("""
                        UPDATE crossword_stats 
                        SET 
                            author = :author,
                            editor = :editor,
                            format_type = :format_type,
                            print_date = :print_date,
                            day_of_week_name = :day_of_week_name,
                            day_of_week_integer = :day_of_week_integer,
                            publish_type = :publish_type,
                            title = :title,
                            version = :version,
                            percent_filled = :percent_filled,
                            solved = :solved,
                            star = :star,
                            solving_seconds = :solving_seconds
                        WHERE puzzle_id = :puzzle_id
                    """)
                    connection.execute(update_stmt, record)
                    updates += 1
                    logging.info(f"Updated puzzle_id: {puzzle_id}")
                else:
                    # Insert new puzzle stats
                    insert_stmt = text("""
                        INSERT INTO crossword_stats 
                        (author, editor, format_type, print_date, day_of_week_name, 
                         day_of_week_integer, publish_type, puzzle_id, title, version, 
                         percent_filled, solved, star, solving_seconds)
                        VALUES 
                        (:author, :editor, :format_type, :print_date, :day_of_week_name,
                         :day_of_week_integer, :publish_type, :puzzle_id, :title, :version,
                         :percent_filled, :solved, :star, :solving_seconds)
                    """)
                    connection.execute(insert_stmt, record)
                    inserts += 1
                    logging.info(f"Inserted new puzzle_id: {puzzle_id}")
                    
                    # Also add initial session if solving time exists
                    if current_solving_seconds:
                        session_insert = text("""
                            INSERT INTO puzzle_sessions 
                            (puzzle_id, session_date, solving_seconds)
                            VALUES (:puzzle_id, :session_date, :solving_seconds)
                        """)
                        connection.execute(session_insert, {
                            "puzzle_id": puzzle_id,
                            "session_date": current_date,
                            "solving_seconds": current_solving_seconds
                        })
                        sessions_added += 1
                        logging.info(f"Added initial session for new puzzle_id {puzzle_id}")
                
            except Exception as e:
                errors += 1
                logging.error(f"Error processing puzzle_id {record.get('puzzle_id', 'unknown')}: {str(e)}")
                logging.error(f"Record data: {record}")
                continue
        
        connection.commit()
    
    logging.info(f"Summary: {inserts} inserts, {updates} updates, {sessions_added} sessions added, {errors} errors")
    return inserts, updates, sessions_added, errors

def update_crossword_stats():
    try:
        args = parse_args()
        
        # Define paths
        DATA_DIR = Path('/app/data')
        DATA_DIR.mkdir(exist_ok=True)
        
        # Calculate date range
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        else:
            start_date = end_date - timedelta(days=args.days_back)
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        update_file = DATA_DIR / f'crossword_update_{timestamp}.csv'
        
        logging.info(f"Starting update for period {start_date.date()} to {end_date.date()}")
        
        # Run the fetch script with debugging
        cmd = [
            'python', 'fetch_puzzle_stats.py',
            '-s', start_date.strftime('%Y-%m-%d'),
            '-e', end_date.strftime('%Y-%m-%d'),
            '-o', str(update_file)
        ]
        logging.info(f"Executing command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"Fetch script stdout: {result.stdout}")
        if result.stderr:
            logging.warning(f"Fetch script stderr: {result.stderr}")
        
        # Read new data
        logging.info("Reading update data")
        update_df = pd.read_csv(update_file)
        logging.info(f"Found {len(update_df)} puzzles in CSV")
        
        try:
            # Upsert to database
            logging.info(f"Attempting to upsert {len(update_df)} rows to database")
            inserts, updates, sessions_added, errors = upsert_crossword_stats(update_df, engine)
            logging.info(f"Successfully wrote data to database: {inserts} inserts, {updates} updates, {sessions_added} sessions, {errors} errors")
        except Exception as e:
            logging.error(f"Database error: {str(e)}")
            raise
            
        # Cleanup temporary files
        if update_file.exists():
            update_file.unlink()
            
    except Exception as e:
        logging.error(f"Error during update: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    update_crossword_stats()