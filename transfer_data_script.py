import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    filename="db_transfer.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Connection strings
source_conn_str = "postgresql+psycopg2://username:password@localhost:5432/database_name"
target_conn_str = "postgresql+psycopg2://username:password@localhost:5432/database_name"

# Create SQLAlchemy engines
source_engine = create_engine(source_conn_str)
target_engine = create_engine(target_conn_str)

# Define source and target schemas
source_schema = "public"  # Replace with your source schema name if needed
target_schema = "omar"

# Tables to exclude from transfer
tables_to_exclude = [
    "public.tenants_tenant",
    "public.tenants_domain",
    "public.analytics_analyticsevent",
    "public.silk_response",
]

# Connect to the source database
source_metadata = MetaData(schema=source_schema)
source_metadata.reflect(bind=source_engine)

# Connect to the target database
target_metadata = MetaData(schema=target_schema)
target_metadata.reflect(bind=target_engine)

# Create a session to interact with the databases
SessionSource = sessionmaker(bind=source_engine)
SessionTarget = sessionmaker(bind=target_engine)

# Function to create missing tables in the target schema
# # Function to create missing tables in the target schema
def create_table_in_target(table_name):
    try:
        # Check if the table is in the exclusion list before proceeding
        if str(table_name) in tables_to_exclude:
            logging.info(f"Skipping table '{table_name}' (excluded from transfer).")
            return None  # Skip the table creation for excluded tables

        # Get the table from source metadata
        source_table = source_metadata.tables.get(table_name)
        if source_table is None:
            logging.warning(
                f"Table '{table_name}' does not exist in the source schema '{source_schema}'"
            )
            return None
        dest_table_name = table_name.split(".")[-1]
        # Define a table structure for the target
        target_table = Table(
            dest_table_name,
            target_metadata,
            *[column.copy() for column in source_table.columns],
            schema=target_schema,
        )

        # Create the table in the target schema
        target_table.create(bind=target_engine, checkfirst=True)
        logging.info(
            f"Table '{dest_table_name}' created in target schema '{target_schema}'"
        )
        return target_table
    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {str(e)}")
        return None


# Function to transfer data in chunks
def transfer_data():
    try:
        # Start a session for the source and target
        session_source = SessionSource()
        session_target = SessionTarget()

        # Define the chunk size for data transfer
        chunk_size = 1000  # Adjust this size based on your database and memory capacity

        # Loop through all tables in the source schema
        for table_name in source_metadata.tables:
            # Ensure the table is not in the exclusion list before proceeding
            if table_name in tables_to_exclude:
                logging.info(f"Excluding table '{table_name}' from data transfer.")
                continue  # Skip tables listed in the exclusion list

            logging.info(f"Processing table '{table_name}'")

            # Ensure the table exists in the target schema
            target_table = create_table_in_target(table_name)
            if target_table is None:
                continue  # Skip if the table creation failed

            # Fetch data in chunks from the source table
            total_rows = session_source.query(
                source_metadata.tables[table_name]
            ).count()
            logging.info(f"Found {total_rows} rows in source table '{table_name}'")

            for offset in range(0, total_rows, chunk_size):
                # Fetch a chunk of data from the source table
                data_chunk = (
                    session_source.query(source_metadata.tables[table_name])
                    .offset(offset)
                    .limit(chunk_size)
                    .all()
                )
                logging.info(
                    f"Fetched {len(data_chunk)} rows from '{table_name}' (offset {offset})"
                )

                # Insert data chunk into the target table
                if data_chunk:
                    for row in data_chunk:
                        row_dict = {
                            col.name: getattr(row, col.name)
                            for col in source_metadata.tables[table_name].columns
                        }
                        session_target.execute(target_table.insert().values(row_dict))
                    logging.info(
                        f"Inserted {len(data_chunk)} rows into '{table_name}' (offset {offset})"
                    )
                else:
                    logging.info(
                        f"No data to insert for '{table_name}' at offset {offset}"
                    )

            # Commit the transaction
            session_target.commit()
            logging.info("Data commited")

        logging.info("Data transfer completed successfully")

    except SQLAlchemyError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        session_target.rollback()  # Rollback any changes in case of error
    finally:
        session_source.close()
        session_target.close()


# Run the transfer function
if __name__ == "__main__":
    transfer_data()
