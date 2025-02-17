import logging
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    UniqueConstraint,
    CheckConstraint,
    ForeignKeyConstraint,
    text,
)
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
source_schema = "public"  # Adjust as needed
target_schema = "test5"  # Adjust target schema if needed

# Tables to exclude from transfer
tables_to_exclude = [
    "public.tenants_tenant",
    "public.tenants_domain",
    "public.analytics_analyticsevent",
    "public.silk_response",
]

# Reflect source metadata
source_metadata = MetaData(schema=source_schema)
source_metadata.reflect(bind=source_engine)

# Prepare target metadata (we will create tables here)
target_metadata = MetaData(schema=target_schema)
# We do NOT reflect target schema here because we’re going to create new tables.

SessionSource = sessionmaker(bind=source_engine)
SessionTarget = sessionmaker(bind=target_engine)


def create_table_without_fks(table_name):
    """
    Create a table in the target schema with columns and constraints except foreign keys.
    Uses extend_existing=True to avoid duplicate table definitions.
    """
    try:
        if table_name in tables_to_exclude:
            logging.info(f"Skipping table '{table_name}' (excluded).")
            return None

        source_table = source_metadata.tables.get(table_name)
        if source_table is None:
            logging.warning(
                f"Table '{table_name}' does not exist in the source schema '{source_schema}'"
            )
            return None

        dest_table_name = table_name.split(".")[-1]
        # Copy columns (this copies primary key and other column-level attributes)
        columns_copy = [column.copy() for column in source_table.columns]

        # Create or extend the table definition in the target metadata.
        target_table = Table(
            dest_table_name,
            target_metadata,
            *columns_copy,
            schema=target_schema,
            extend_existing=True,  # This avoids the "table already defined" error.
        )

        # Rebuild Unique and Check constraints
        for constraint in source_table.constraints:
            cname = constraint.name
            if constraint.__class__.__name__ == "PrimaryKeyConstraint":
                continue  # handled automatically by column.primary_key
            elif constraint.__class__.__name__ == "UniqueConstraint":
                target_cols = [target_table.c[col.name] for col in constraint.columns]
                uc = UniqueConstraint(*target_cols, name=cname)
                target_table.append_constraint(uc)
            elif constraint.__class__.__name__ == "CheckConstraint":
                cc = CheckConstraint(constraint.sqltext, name=cname)
                target_table.append_constraint(cc)
            # Skip foreign keys here

        # Create the table in target
        target_table.create(bind=target_engine, checkfirst=True)
        logging.info(
            f"Table '{dest_table_name}' created in target schema '{target_schema}' (without FKs)."
        )
        return target_table
    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {str(e)}")
        return None


def add_foreign_keys():
    """
    After all tables are created, add foreign key constraints to the target tables.
    """
    for table_name in source_metadata.tables:
        if table_name in tables_to_exclude:
            continue

        source_table = source_metadata.tables[table_name]
        dest_table_name = table_name.split(".")[-1]

        # Check if the table exists in target metadata
        if dest_table_name not in target_metadata.tables:
            logging.warning(
                f"Target table '{dest_table_name}' not found. Skipping FK addition."
            )
            continue

        for constraint in source_table.constraints:
            if constraint.__class__.__name__ != "ForeignKeyConstraint":
                continue

            # Build lists of local and remote columns
            local_column_names = [col.name for col in constraint.columns]
            # Build remote reference: assume referenced table is also in target_schema
            remote_columns = []
            for element in constraint.elements:
                ref_table_name = element.column.table.name
                ref_column_name = element.column.name
                # Fully qualified reference in the target database.
                remote_columns.append(
                    f"{target_schema}.{ref_table_name}.{ref_column_name}"
                )

            # Construct the ALTER TABLE statement manually.
            local_cols_sql = ", ".join(local_column_names)
            # Assuming all elements reference the same table:
            ref_table = constraint.elements[0].column.table.name
            remote_cols = ", ".join(
                [element.column.name for element in constraint.elements]
            )

            ondelete_clause = (
                f" ON DELETE {constraint.ondelete}" if constraint.ondelete else ""
            )
            onupdate_clause = (
                f" ON UPDATE {constraint.onupdate}" if constraint.onupdate else ""
            )

            alter_stmt = text(
                f"""
                ALTER TABLE {target_schema}.{dest_table_name}
                ADD CONSTRAINT {constraint.name}
                FOREIGN KEY ({local_cols_sql})
                REFERENCES {target_schema}.{ref_table} ({remote_cols})
                {ondelete_clause} {onupdate_clause};
                """
            )

            try:
                target_engine.execute(alter_stmt)
                logging.info(
                    f"Foreign key constraint '{constraint.name}' added to table '{dest_table_name}'."
                )
            except Exception as e:
                logging.error(
                    f"Error adding FK '{constraint.name}' on table '{dest_table_name}': {str(e)}"
                )


def transfer_data():
    """
    Transfer data in chunks from source tables to target tables.
    """
    session_source = SessionSource()
    session_target = SessionTarget()
    chunk_size = 1000

    try:
        for table_name in source_metadata.tables:
            if table_name in tables_to_exclude:
                logging.info(f"Excluding table '{table_name}' from data transfer.")
                continue

            logging.info(f"Processing data for table '{table_name}'")
            source_table = source_metadata.tables[table_name]
            dest_table_name = table_name.split(".")[-1]

            # Ensure table exists (if not, create it without FKs)
            target_table = target_metadata.tables.get(dest_table_name)
            if target_table is None:
                target_table = create_table_without_fks(table_name)
                if target_table is None:
                    continue

            total_rows = session_source.query(source_table).count()
            logging.info(f"Found {total_rows} rows in source table '{table_name}'")

            for offset in range(0, total_rows, chunk_size):
                data_chunk = (
                    session_source.query(source_table)
                    .offset(offset)
                    .limit(chunk_size)
                    .all()
                )
                logging.info(
                    f"Fetched {len(data_chunk)} rows from '{table_name}' (offset {offset})"
                )

                if data_chunk:
                    for row in data_chunk:
                        row_dict = {
                            col.name: getattr(row, col.name)
                            for col in source_table.columns
                        }
                        session_target.execute(target_table.insert().values(row_dict))
                    logging.info(
                        f"Inserted {len(data_chunk)} rows into '{dest_table_name}' (offset {offset})"
                    )
            session_target.commit()
            logging.info(f"Data committed for table '{dest_table_name}'")

        logging.info("Data transfer completed successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        session_target.rollback()
    finally:
        session_source.close()
        session_target.close()


if __name__ == "__main__":
    # PHASE 1: Create all tables without foreign key constraints.
    for table_name in source_metadata.tables:
        if table_name in tables_to_exclude:
            continue
        # Create table only if not already defined in metadata.
        dest_table_name = table_name.split(".")[-1]
        if dest_table_name not in target_metadata.tables:
            create_table_without_fks(table_name)
        else:
            logging.info(
                f"Table '{dest_table_name}' already exists in target metadata."
            )

    # Transfer data after tables are created.
    transfer_data()

    # PHASE 2: Add foreign key constraints (now that all tables exist).
    add_foreign_keys()
