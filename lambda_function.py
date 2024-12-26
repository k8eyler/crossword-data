import pytz
from datetime import datetime, timedelta
from sqlalchemy import text
import logging
import pandas as pd

def upsert_crossword_stats(df, engine):
    """
    Upsert crossword stats and track solving sessions with timezone-aware timestamps.
    """
    # Clean the DataFrame
    df = df.copy()
    current_timestamp = datetime.now(pytz.UTC)
    current_date = current_timestamp.date()
    
    # Type conversions
    type_conversions = {
        'puzzle_id': 'int64',
        'day_of_week_integer': 'int32',
        'version': 'int32',
        'percent_filled': 'float64',
        'solved': 'bool',
        'solving_seconds': 'Int64'
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
    actual_updates = 0
    inserts = 0
    sessions_added = 0
    errors = 0
    
    with engine.connect() as connection:
        for record in records:
            try:
                puzzle_id = record['puzzle_id']
                current_solving_seconds = record.get('solving_seconds', 0) or 0
                
                # Get previous puzzle state
                check_existing = text("""
                    SELECT solving_seconds, created_at AT TIME ZONE 'UTC' as created_at
                    FROM crossword_stats
                    WHERE puzzle_id = :puzzle_id
                """)
                existing = connection.execute(
                    check_existing,
                    {"puzzle_id": puzzle_id}
                ).first()

                if existing:
                    previous_solving_seconds = existing.solving_seconds or 0
                    
                    # Only create a session if there's new solving time
                    if current_solving_seconds > previous_solving_seconds:
                        new_session_seconds = current_solving_seconds - previous_solving_seconds
                        session_insert = text("""
                            INSERT INTO puzzle_sessions 
                            (puzzle_id, session_date, solving_seconds)
                            VALUES (
                                :puzzle_id, 
                                :session_date AT TIME ZONE 'UTC',
                                :solving_seconds
                            )
                        """)
                        connection.execute(session_insert, {
                            "puzzle_id": puzzle_id,
                            "session_date": current_timestamp,
                            "solving_seconds": new_session_seconds
                        })
                        sessions_added += 1
                        logging.info(f"Added new session for puzzle_id {puzzle_id}: {new_session_seconds} seconds")

                    # Update puzzle stats if needed
                    update_stmt = text("""
                        UPDATE crossword_stats 
                        SET 
                            author = CASE WHEN :author IS DISTINCT FROM author THEN :author ELSE author END,
                            editor = CASE WHEN :editor IS DISTINCT FROM editor THEN :editor ELSE editor END,
                            format_type = CASE WHEN :format_type IS DISTINCT FROM format_type THEN :format_type ELSE format_type END,
                            print_date = CASE WHEN :print_date IS DISTINCT FROM print_date THEN :print_date ELSE print_date END,
                            day_of_week_name = CASE WHEN :day_of_week_name IS DISTINCT FROM day_of_week_name THEN :day_of_week_name ELSE day_of_week_name END,
                            day_of_week_integer = CASE WHEN :day_of_week_integer IS DISTINCT FROM day_of_week_integer THEN :day_of_week_integer ELSE day_of_week_integer END,
                            publish_type = CASE WHEN :publish_type IS DISTINCT FROM publish_type THEN :publish_type ELSE publish_type END,
                            title = CASE WHEN :title IS DISTINCT FROM title THEN :title ELSE title END,
                            version = CASE WHEN :version IS DISTINCT FROM version THEN :version ELSE version END,
                            percent_filled = CASE WHEN :percent_filled IS DISTINCT FROM percent_filled THEN :percent_filled ELSE percent_filled END,
                            solved = CASE WHEN :solved IS DISTINCT FROM solved THEN :solved ELSE solved END,
                            star = CASE WHEN :star IS DISTINCT FROM star THEN :star ELSE star END,
                            solving_seconds = CASE WHEN :solving_seconds IS DISTINCT FROM solving_seconds THEN :solving_seconds ELSE solving_seconds END,
                            last_updated_at = CASE 
                                WHEN :solving_seconds IS DISTINCT FROM solving_seconds 
                                THEN CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                                ELSE last_updated_at 
                            END
                        WHERE puzzle_id = :puzzle_id
                        RETURNING CASE 
                            WHEN xmax::text::int > 0 THEN 1
                            ELSE 0
                        END as was_updated
                    """)
                    result = connection.execute(update_stmt, record).scalar()
                    updates += 1
                    if result == 1:
                        actual_updates += 1
                        logging.info(f"Updated puzzle_id: {puzzle_id} (data changed)")
                    else:
                        logging.info(f"Checked puzzle_id: {puzzle_id} (no changes)")
                else:
                    # Insert new puzzle stats
                    insert_stmt = text("""
                        INSERT INTO crossword_stats 
                        (author, editor, format_type, print_date, day_of_week_name, 
                         day_of_week_integer, publish_type, puzzle_id, title, version, 
                         percent_filled, solved, star, solving_seconds, created_at, last_updated_at)
                        VALUES 
                        (:author, :editor, :format_type, :print_date, :day_of_week_name,
                         :day_of_week_integer, :publish_type, :puzzle_id, :title, :version,
                         :percent_filled, :solved, :star, :solving_seconds, 
                         CURRENT_TIMESTAMP AT TIME ZONE 'UTC', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                    """)
                    connection.execute(insert_stmt, record)
                    inserts += 1
                    logging.info(f"Inserted new puzzle_id: {puzzle_id}")
                    
                    # Add initial session for new puzzles with solving time
                    if current_solving_seconds > 0:
                        session_insert = text("""
                            INSERT INTO puzzle_sessions 
                            (puzzle_id, session_date, solving_seconds)
                            VALUES (
                                :puzzle_id, 
                                :session_date AT TIME ZONE 'UTC',
                                :solving_seconds
                            )
                        """)
                        connection.execute(session_insert, {
                            "puzzle_id": puzzle_id,
                            "session_date": current_timestamp,
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
    
    logging.info(f"Summary: {inserts} inserts, {updates} checks, {actual_updates} actual updates, {sessions_added} sessions added, {errors} errors")
    return inserts, updates, actual_updates, sessions_added, errors