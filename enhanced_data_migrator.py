import logging
import os
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    UniqueConstraint,
    CheckConstraint,
    ForeignKeyConstraint,
    text,
    inspect,
)
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError

# --- Configuration ---
# Read from environment variables with defaults
SOURCE_DB_URL = os.environ.get("SOURCE_DB_URL", "postgresql+psycopg2://user:pass@host:port/sourcedb")
TARGET_DB_URL = os.environ.get("TARGET_DB_URL", "postgresql+psycopg2://user:pass@host:port/targetdb")
SOURCE_SCHEMA = os.environ.get("SOURCE_SCHEMA", "public")
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "public")
# Comma-separated string from env var, then split into a list
EXCLUDE_TABLES_STR = os.environ.get("EXCLUDE_TABLES", "")
TABLES_TO_EXCLUDE = [table.strip() for table in EXCLUDE_TABLES_STR.split(',') if table.strip()] \
    if EXCLUDE_TABLES_STR else []

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
LOG_LEVEL_STR = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

# --- Logging Configuration ---
logging.basicConfig(
    filename="db_migration_enhanced.log",
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)


class DatabaseConnector:
    """Handles database engine and session creation."""
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = None
        try:
            self.engine = create_engine(db_url)
            logging.info(f"Database engine created for URL (ending with): ...{db_url[-20:]}")
        except Exception as e:
            logging.exception(f"Failed to create database engine for {db_url}: {e}")
            raise
        self.SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

    def get_session(self):
        """Provides a new database session."""
        try:
            session = self.SessionLocal()
            logging.debug(f"Database session created for ...{self.db_url[-20:]}")
            return session
        except Exception as e:
            logging.exception(f"Failed to create session for ...{self.db_url[-20:]}: {e}")
            raise

    def get_engine(self):
        """Returns the database engine."""
        return self.engine

    def dispose_engine(self):
        """Disposes of the engine's connection pool."""
        if self.engine:
            self.engine.dispose()
            logging.info(f"Database engine disposed for ...{self.db_url[-20:]}")


class SchemaManager:
    """Manages schema reflection, table creation, and constraint handling."""
    def __init__(self, source_engine, target_engine, source_schema: str, target_schema: str, tables_to_exclude: list):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.tables_to_exclude = tables_to_exclude
        self.source_metadata = MetaData(schema=self.source_schema)
        self.target_metadata = MetaData(schema=self.target_schema)

    def reflect_source_schema(self):
        """Reflects the source database schema."""
        try:
            logging.info(f"Reflecting source schema '{self.source_schema}'...")
            self.source_metadata.reflect(bind=self.source_engine)
            logging.info(f"Source schema '{self.source_schema}' reflected successfully. Found {len(self.source_metadata.tables)} tables.")
        except SQLAlchemyError as e:
            logging.exception(f"Error reflecting source schema '{self.source_schema}': {e}")
            raise

    def create_tables_without_fks(self):
        """Creates tables in the target schema without foreign keys."""
        logging.info("Starting phase: Create tables without Foreign Keys.")
        created_tables_count = 0
        for table_name_qualified, source_table_obj in self.source_metadata.tables.items():
            table_name = source_table_obj.name
            if f"{self.source_schema}.{table_name}" in self.tables_to_exclude or table_name in self.tables_to_exclude:
                logging.info(f"Skipping table '{table_name_qualified}' as it is in the exclusion list.")
                continue

            logging.info(f"Processing table for creation (without FKs): '{self.target_schema}.{table_name}'")

            # Ensure table name for target_metadata does not include schema if schema is already set for target_metadata
            target_table_name_in_metadata = table_name

            try:
                columns_copy = []
                for col in source_table_obj.columns:
                    columns_copy.append(col.copy())

                target_table = Table(
                    target_table_name_in_metadata,
                    self.target_metadata,
                    *columns_copy,
                    schema=self.target_schema  # Explicitly set schema for the Table object
                )

                for constraint in source_table_obj.constraints:
                    if isinstance(constraint, UniqueConstraint):
                        target_cols = [target_table.c[col.name] for col in constraint.columns]
                        uc = UniqueConstraint(*target_cols, name=constraint.name)
                        target_table.append_constraint(uc)
                    elif isinstance(constraint, CheckConstraint):
                        cc = CheckConstraint(constraint.sqltext, name=constraint.name)
                        target_table.append_constraint(cc)

                target_table.create(bind=self.target_engine, checkfirst=True)
                logging.info(f"Table '{self.target_schema}.{table_name}' created successfully (without FKs).")
                created_tables_count += 1
            except SQLAlchemyError as e:
                logging.error(f"Error creating table '{self.target_schema}.{table_name}': {e}", exc_info=True)
            except Exception as e: # Catch other potential errors
                logging.error(f"An unexpected error occurred while creating table '{self.target_schema}.{table_name}': {e}", exc_info=True)

        logging.info(f"Phase 'Create tables without Foreign Keys' completed. {created_tables_count} tables processed for creation.")
        # Re-reflect target metadata to include newly created tables
        self.target_metadata.reflect(bind=self.target_engine, schema=self.target_schema)


    def add_foreign_keys(self):
        """Adds foreign key constraints to the tables in the target schema."""
        logging.info("Starting phase: Add Foreign Key constraints.")
        added_fks_count = 0
        inspector = inspect(self.source_engine) # Inspector for source to get FK details

        for table_name_qualified, source_table_obj in self.source_metadata.tables.items():
            source_simple_table_name = source_table_obj.name

            if f"{self.source_schema}.{source_simple_table_name}" in self.tables_to_exclude or \
               source_simple_table_name in self.tables_to_exclude:
                logging.debug(f"Skipping FKs for excluded table '{table_name_qualified}'.")
                continue

            target_table_key = f"{self.target_schema}.{source_simple_table_name}"
            if target_table_key not in self.target_metadata.tables:
                logging.warning(f"Target table '{target_table_key}' not found in target metadata. Skipping FK addition.")
                continue

            logging.debug(f"Processing FKs for table '{target_table_key}'.")

            try:
                # Get FKs from source table using inspector for more reliable details
                source_fks = inspector.get_foreign_keys(source_simple_table_name, schema=self.source_schema)

                for fk_constraint in source_fks:
                    fk_name = fk_constraint['name']
                    local_column_names = fk_constraint['constrained_columns']

                    # Referenced details
                    referred_schema = fk_constraint.get('referred_schema', self.target_schema) # Assume same schema if not specified
                    referred_table_name = fk_constraint['referred_table']
                    referred_column_names = fk_constraint['referred_columns']

                    # Construct remote reference for SQLAlchemy: schema.table.column
                    remote_cols_sql_alchemy = [
                        f"{referred_schema}.{referred_table_name}.{col_name}" for col_name in referred_column_names
                    ]

                    # Construct parts for raw SQL ALTER statement
                    local_cols_sql = ", ".join(f'"{c}"' for c in local_column_names) # Quote column names
                    ref_table_sql = f'"{referred_schema}"."{referred_table_name}"' # Quote schema and table
                    remote_cols_sql = ", ".join(f'"{c}"' for c in referred_column_names) # Quote column names

                    ondelete_clause = f" ON DELETE {fk_constraint.get('options', {}).get('ondelete','NO ACTION')}" \
                        if fk_constraint.get('options', {}).get('ondelete') else ""
                    onupdate_clause = f" ON UPDATE {fk_constraint.get('options', {}).get('onupdate','NO ACTION')}" \
                        if fk_constraint.get('options', {}).get('onupdate') else ""

                    # Using raw SQL for adding constraints as it's often more reliable for complex FKs / existing tables
                    # Ensure target table is also quoted
                    alter_stmt_sql = f"""
                        ALTER TABLE "{self.target_schema}"."{source_simple_table_name}"
                        ADD CONSTRAINT "{fk_name}"
                        FOREIGN KEY ({local_cols_sql})
                        REFERENCES {ref_table_sql} ({remote_cols_sql})
                        {onupdate_clause} {ondelete_clause};
                    """
                    try:
                        with self.target_engine.connect() as connection:
                            with connection.begin(): # Start a transaction for the ALTER statement
                                connection.execute(text(alter_stmt_sql))
                        logging.info(f"Foreign key '{fk_name}' added to table '{target_table_key}'.")
                        added_fks_count += 1
                    except SQLAlchemyError as e_alter:
                        # Check if constraint already exists (common case if script is re-run)
                        if "already exists" in str(e_alter).lower():
                             logging.warning(f"Foreign key '{fk_name}' on table '{target_table_key}' likely already exists: {e_alter}")
                        else:
                            logging.error(f"Error adding FK '{fk_name}' to table '{target_table_key}': {e_alter}", exc_info=True)
                    except Exception as e_unexpected_alter:
                        logging.error(f"Unexpected error adding FK '{fk_name}' to table '{target_table_key}': {e_unexpected_alter}", exc_info=True)

            except Exception as e_table_fks:
                logging.error(f"Error processing FKs for table '{target_table_key}': {e_table_fks}", exc_info=True)

        logging.info(f"Phase 'Add Foreign Key constraints' completed. {added_fks_count} FKs processed for addition.")


class DataMigrator:
    """Handles the actual data transfer between source and target databases."""
    def __init__(self, source_connector: DatabaseConnector, target_connector: DatabaseConnector,
                 source_metadata: MetaData, target_metadata: MetaData,
                 source_schema: str, target_schema: str,
                 tables_to_exclude: list, chunk_size: int):
        self.source_connector = source_connector
        self.target_connector = target_connector
        self.source_metadata = source_metadata
        self.target_metadata = target_metadata
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.tables_to_exclude = tables_to_exclude
        self.chunk_size = chunk_size

    def transfer_data_for_all_tables(self):
        """Transfers data for all relevant tables from source to target."""
        logging.info("Starting phase: Data Transfer.")
        source_session = self.source_connector.get_session()
        target_session = self.target_connector.get_session()

        try:
            for table_name_qualified, source_table_obj in self.source_metadata.tables.items():
                simple_table_name = source_table_obj.name

                if f"{self.source_schema}.{simple_table_name}" in self.tables_to_exclude or \
                   simple_table_name in self.tables_to_exclude:
                    logging.info(f"Skipping data transfer for excluded table: '{table_name_qualified}'")
                    continue

                logging.info(f"Processing data transfer for table: '{table_name_qualified}' -> '{self.target_schema}.{simple_table_name}'")

                target_table_key = f"{self.target_schema}.{simple_table_name}"
                target_table_obj = self.target_metadata.tables.get(target_table_key)

                if target_table_obj is None:
                    logging.warning(f"Target table '{target_table_key}' not found in target metadata. Skipping data transfer.")
                    continue

                try:
                    # Optional: Truncate target table before loading (make this configurable if needed)
                    # logging.info(f"Truncating target table '{target_table_key}' before data transfer.")
                    # target_session.execute(text(f'TRUNCATE TABLE "{self.target_schema}"."{simple_table_name}" RESTART IDENTITY CASCADE;'))
                    # target_session.commit()
                    # logging.info(f"Target table '{target_table_key}' truncated.")

                    total_rows = source_session.query(source_table_obj).count()
                    logging.info(f"Found {total_rows} rows in source table '{table_name_qualified}'. Transferring in chunks of {self.chunk_size}.")

                    for offset in range(0, total_rows, self.chunk_size):
                        data_chunk = source_session.query(source_table_obj).offset(offset).limit(self.chunk_size).all()

                        if not data_chunk:
                            logging.debug(f"No more data to fetch from '{table_name_qualified}' at offset {offset}.")
                            break

                        logging.debug(f"Fetched {len(data_chunk)} rows from '{table_name_qualified}' (offset {offset}).")

                        # Convert rows to dicts for insertion
                        chunk_to_insert = []
                        for row in data_chunk:
                            row_dict = {col.name: getattr(row, col.name) for col in source_table_obj.columns}
                            chunk_to_insert.append(row_dict)

                        if chunk_to_insert:
                            target_session.execute(target_table_obj.insert(), chunk_to_insert)
                            logging.debug(f"Prepared to insert {len(chunk_to_insert)} rows into '{target_table_key}' (offset {offset}).")

                    target_session.commit()
                    logging.info(f"Data committed for table '{target_table_key}'. Total {total_rows} rows processed.")

                except SQLAlchemyError as e_transfer:
                    logging.error(f"Error during data transfer for table '{table_name_qualified}': {e_transfer}", exc_info=True)
                    target_session.rollback()
                    logging.info(f"Rolled back transaction for table '{target_table_key}' due to error.")
                except Exception as e_unexpected_transfer:
                    logging.error(f"Unexpected error during data transfer for table '{table_name_qualified}': {e_unexpected_transfer}", exc_info=True)
                    target_session.rollback()

            logging.info("Phase 'Data Transfer' completed for all tables.")

        finally:
            if source_session:
                source_session.close()
                logging.debug("Source session closed.")
            if target_session:
                target_session.close()
                logging.debug("Target session closed.")


def main_migration_process():
    """Orchestrates the entire migration process."""
    logging.info("--- Starting Enhanced PostgreSQL Data Migration Process ---")
    logging.info(f"Source DB URL (ends with): ...{SOURCE_DB_URL[-20:]}")
    logging.info(f"Target DB URL (ends with): ...{TARGET_DB_URL[-20:]}")
    logging.info(f"Source Schema: {SOURCE_SCHEMA}, Target Schema: {TARGET_SCHEMA}")
    logging.info(f"Tables to Exclude: {TABLES_TO_EXCLUDE if TABLES_TO_EXCLUDE else 'None'}")
    logging.info(f"Chunk Size: {CHUNK_SIZE}")
    logging.info(f"Log Level: {LOG_LEVEL_STR}")

    source_connector = None
    target_connector = None

    try:
        # 1. Setup Database Connections
        logging.info("Step 1: Setting up database connections...")
        source_connector = DatabaseConnector(SOURCE_DB_URL)
        target_connector = DatabaseConnector(TARGET_DB_URL)
        logging.info("Database connections established.")

        # 2. Initialize Schema Manager
        logging.info("Step 2: Initializing Schema Manager...")
        schema_manager = SchemaManager(
            source_engine=source_connector.get_engine(),
            target_engine=target_connector.get_engine(),
            source_schema=SOURCE_SCHEMA,
            target_schema=TARGET_SCHEMA,
            tables_to_exclude=TABLES_TO_EXCLUDE
        )
        logging.info("Schema Manager initialized.")

        # 3. Reflect Source Schema
        logging.info("Step 3: Reflecting source schema...")
        schema_manager.reflect_source_schema()
        logging.info("Source schema reflection complete.")

        # 4. Create Tables in Target (without FKs)
        logging.info("Step 4: Creating tables in target (without FKs)...")
        schema_manager.create_tables_without_fks()
        logging.info("Table creation (without FKs) complete.")

        # 5. Initialize Data Migrator
        logging.info("Step 5: Initializing Data Migrator...")
        data_migrator = DataMigrator(
            source_connector=source_connector,
            target_connector=target_connector,
            source_metadata=schema_manager.source_metadata, # Use reflected source metadata
            target_metadata=schema_manager.target_metadata, # Use target metadata (now with created tables)
            source_schema=SOURCE_SCHEMA,
            target_schema=TARGET_SCHEMA,
            tables_to_exclude=TABLES_TO_EXCLUDE,
            chunk_size=CHUNK_SIZE
        )
        logging.info("Data Migrator initialized.")

        # 6. Transfer Data
        logging.info("Step 6: Transferring data...")
        data_migrator.transfer_data_for_all_tables()
        logging.info("Data transfer complete.")

        # 7. Add Foreign Keys to Target Tables
        logging.info("Step 7: Adding Foreign Keys to target tables...")
        schema_manager.add_foreign_keys() # FKs are added using the schema_manager
        logging.info("Foreign Key addition complete.")

        logging.info("--- Enhanced PostgreSQL Data Migration Process Completed Successfully ---")

    except Exception as e:
        logging.critical(f"A critical error occurred during the migration process: {e}", exc_info=True)
        logging.critical("--- Enhanced PostgreSQL Data Migration Process Failed ---")
    finally:
        logging.info("Step 8: Disposing database engine connections...")
        if source_connector:
            source_connector.dispose_engine()
        if target_connector:
            target_connector.dispose_engine()
        logging.info("Database engine connections disposed.")
        logging.info("--- Migration script execution finished ---")


if __name__ == "__main__":
    # Example of how to set environment variables before running, if not set externally:
    # os.environ["SOURCE_DB_URL"] = "postgresql+psycopg2://user:pass@host1:5432/sourcedb"
    # os.environ["TARGET_DB_URL"] = "postgresql+psycopg2://user:pass@host2:5432/targetdb"
    # os.environ["SOURCE_SCHEMA"] = "public"
    # os.environ["TARGET_SCHEMA"] = "migrated_schema"
    # os.environ["EXCLUDE_TABLES"] = "alembic_version,temp_table"
    # os.environ["LOG_LEVEL"] = "DEBUG"

    main_migration_process()
