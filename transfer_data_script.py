import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
import logging
import json

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('data_transfer.log')
console_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def ensure_schema_exists(engine, schema_name):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}')"))
        schema_exists = result.fetchone()[0]
        if not schema_exists:
            connection.execute(text(f"CREATE SCHEMA {schema_name}"))
            logger.info(f"Schema {schema_name} created.")
        else:
            logger.info(f"Schema {schema_name} already exists.")

def convert_to_json(x, table_name, col):
    try:
        if isinstance(x, dict):
            return json.dumps(x)
        elif isinstance(x, list) or isinstance(x, np.ndarray):  # Convert lists or NumPy arrays to JSON
            return json.dumps(x.tolist() if isinstance(x, np.ndarray) else x)
        elif pd.isna(x):
            return None
        else:
            return x
    except TypeError:
        logger.error(f"Failed to serialize value {x} in column {col} of table {table_name}. Replacing with NULL.")
        return None

def sanitize_dataframe(df, table_name):
    """
    Cleans DataFrame by:
    - Converting NumPy arrays and lists to JSON strings.
    - Converting dictionaries to JSON strings.
    - Handling ambiguous NumPy truth values.
    """
    for col in df.columns:
        try:
            # Convert NumPy arrays and lists to JSON strings
            if df[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any():
                df[col] = df[col].apply(lambda x: convert_to_json(x, table_name, col))

            # Convert dictionaries to JSON strings
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

            # Ensure no ambiguous NumPy truth evaluations
            df[col] = df[col].apply(lambda x: None if isinstance(x, np.ndarray) and x.size == 0 else x)

        except Exception as e:
            logger.error(f"Error processing column {col} in table {table_name}: {e}")
    return df


def transfer_table(src_engine, dst_engine, table_name, schema_name, if_exists='replace'):
    try:
        chunk_size = 1000
        first_chunk = True
        for chunk in pd.read_sql(f"SELECT * FROM {table_name}", src_engine, chunksize=chunk_size):
            chunk = sanitize_dataframe(chunk, table_name)  # Ensure data is properly formatted
            if first_chunk:
                chunk.to_sql(table_name, dst_engine, if_exists=if_exists, index=False, schema=schema_name, method='multi')
                first_chunk = False
            else:
                chunk.to_sql(table_name, dst_engine, if_exists='append', index=False, schema=schema_name, method='multi')
            logger.info(f"Chunk of table {table_name} transferred successfully to schema {schema_name}.")
        logger.info(f"All chunks of table {table_name} transferred successfully to schema {schema_name}.")
    except Exception as e:
        logger.error(f"Error transferring table {table_name}: {e}")

def main():
    source_conn_str = "postgresql+psycopg2://username:password@localhost:5432/database_name"
    target_conn_str = "postgresql+psycopg2://username:password@localhost:5432/database_name"
    
    src_engine = create_engine(source_conn_str)
    dst_engine = create_engine(target_conn_str)
    
    dest_schema = 'kamal123'
    ensure_schema_exists(dst_engine, dest_schema)
    
    inspector = inspect(src_engine)
    tables = inspector.get_table_names(schema='public')
    tables_to_exclude = [
        'tenants_tenant', 'tenants_domain', 'analytics_analyticsevent', 'silk_response', 
        'question_bank_historicalquestion', 'slo_historicallessonplan', 'book_library_historicalbookchapterlessonplan', 
        'schools_historicaluserannouncement','silk_sqlquery','silk_request','silk_profile_queries'
    ]
    tables_to_transfer = [table for table in tables if table not in tables_to_exclude]
    
    for table_name in tables_to_transfer:
        logger.info(f"Starting transfer for table {table_name} to schema {dest_schema}.")
        transfer_table(src_engine, dst_engine, table_name, dest_schema, if_exists='replace')
        logger.info(f"Transfer completed for table {table_name} to schema {dest_schema}.")
    
    logger.info("All tables processed successfully.")

if __name__ == "__main__":
    main()
