# PostgreSQL Data Migrator

PostgreSQL Data Migrator is a powerful and efficient suite of Python scripts designed to seamlessly transfer tables and their data between PostgreSQL databases. It supports schema reflection, creation, data migration in chunks, and handling of constraints. Ideal for database replication, ETL processes, and cross-environment migrations. 🚀

## Features

*   **Flexible Data Transfer:** Choose between a basic script or an advanced script with deferred constraint handling.
*   **Schema Management:**
    *   Automatic reflection of source database schema.
    *   Creation of tables in the target database.
    *   (Advanced Script) Handles primary keys, unique constraints, check constraints, and foreign keys (deferred).
*   **Bulk Data Transfer:** Efficiently transfers data in configurable chunks.
*   **Table Exclusions:** Easily specify tables to exclude from the migration process.
*   **Comprehensive Logging:** Detailed logs (`db_transfer.log`) for monitoring and debugging.
*   **SQL Utilities:** Includes helper SQL scripts for tasks like table creation examples and sequence resetting.

## Prerequisites

*   Python 3.7+
*   PostgreSQL server access for both source and target databases.
*   `pip` for installing Python packages.

## Installation

1.  **Clone the repository (or download the files):**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install required Python packages:**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Configuration for the Python scripts is primarily done by editing the script files directly. Future enhancements may move this to environment variables or configuration files.

**Key variables to configure at the top of each Python script (`transfer_data_script.py`, `transfer_data_with_constraints_script.py`, and the upcoming `enhanced_data_migrator.py`):**

*   `source_conn_str`: SQLAlchemy connection string for your source PostgreSQL database.
    *   Format: `postgresql+psycopg2://username:password@host:port/database_name`
*   `target_conn_str`: SQLAlchemy connection string for your target PostgreSQL database.
    *   Format: `postgresql+psycopg2://username:password@host:port/database_name`
*   `source_schema`: The name of the schema in the source database from which to transfer tables (e.g., `"public"`).
*   `target_schema`: The name of the schema in the target database where tables and data will be transferred (e.g., `"public"`).
*   `tables_to_exclude`: A Python list of table names (strings) to exclude from the transfer process.
    *   Example: `tables_to_exclude = ["alembic_version", "spatial_ref_sys"]`

**Important Security Note:** Avoid hardcoding sensitive credentials directly in scripts for production environments. Consider using environment variables or a secure secrets management system. The `enhanced_data_migrator.py` script will demonstrate using environment variables.

## Script Descriptions and Usage

### 1. `transfer_data_script.py`

*   **Description:** A basic script that transfers tables and data. It creates tables in the target database based on the source schema but does not explicitly handle complex constraints like foreign keys during the initial table creation. This might require tables to be created in a specific order if there are interdependencies not handled by simple reflection.
*   **Usage:**
    1.  Configure the connection strings and other parameters at the top of the script.
    2.  Run the script:
        ```bash
        python transfer_data_script.py
        ```

### 2. `transfer_data_with_constraints_script.py`

*   **Description:** An advanced script designed to handle database constraints more robustly. It follows a multi-phase approach:
    1.  Creates tables in the target schema *without* foreign key constraints (but includes other constraints like PK, Unique, Check).
    2.  Transfers data in chunks.
    3.  Adds foreign key constraints to the target tables using `ALTER TABLE` statements.
    This approach helps avoid issues with table creation order due to foreign key dependencies.
*   **Usage:**
    1.  Configure the connection strings and other parameters at the top of the script.
    2.  Run the script:
        ```bash
        python transfer_data_with_constraints_script.py
        ```

### 3. `enhanced_data_migrator.py`

*   **Description:** A refactored and enhanced script for robust database migrations. It features:
    *   Configuration primarily via environment variables for better security and flexibility.
    *   A modular, object-oriented design (Connectors, Schema Manager, Data Migrator).
    *   Improved error handling, detailed logging (to `db_migration_enhanced.log` and console).
    *   Follows the reliable deferred foreign key constraint approach similar to `transfer_data_with_constraints_script.py`.
*   **Configuration (Environment Variables):**
    *   `SOURCE_DB_URL`: SQLAlchemy URL for the source database.
        *   Default: `postgresql+psycopg2://user:pass@host:port/sourcedb`
    *   `TARGET_DB_URL`: SQLAlchemy URL for the target database.
        *   Default: `postgresql+psycopg2://user:pass@host:port/targetdb`
    *   `SOURCE_SCHEMA`: Schema name in the source database.
        *   Default: `public`
    *   `TARGET_SCHEMA`: Schema name in the target database.
        *   Default: `public`
    *   `EXCLUDE_TABLES`: Comma-separated string of table names to exclude (e.g., `"table1,table2"`).
        *   Default: "" (none excluded)
    *   `CHUNK_SIZE`: Number of rows to process per data transfer chunk.
        *   Default: `1000`
    *   `LOG_LEVEL`: Logging level (e.g., `DEBUG`, `INFO`, `WARNING`).
        *   Default: `INFO`
*   **Usage:**
    1.  Set the required environment variables (see above). For example:
        ```bash
        export SOURCE_DB_URL="postgresql+psycopg2://myuser:mypass@sourcehost:5432/mydb"
        export TARGET_DB_URL="postgresql+psycopg2://myuser:mypass@targethost:5432/mydb_migrated"
        export SOURCE_SCHEMA="public"
        export TARGET_SCHEMA="migrated"
        export EXCLUDE_TABLES="alembic_version,spatial_ref_sys"
        export LOG_LEVEL="DEBUG"
        ```
    2.  Run the script:
        ```bash
        python enhanced_data_migrator.py
        ```
    *   Logs will be output to `db_migration_enhanced.log` and the console.

## SQL Scripts

The repository includes SQL scripts that can be useful in conjunction with the migration process. Execute these using a PostgreSQL client like `psql` or a GUI tool (e.g., DBeaver, pgAdmin).

### 1. `event-table.sql`

*   **Description:** Contains a `CREATE TABLE` statement for a sample table named `omar.analytics_analyticsevent`. This can serve as an example or a template. It also shows how to optionally add foreign key constraints.
*   **When to use:**
    *   As a reference for SQL table definitions.
    *   If you need to manually create this specific table before or after a migration.

### 2. `after-running-script.sql`

*   **Description:** This script contains PL/pgSQL blocks to reset PostgreSQL sequences (for columns like `id` and `history_id`) to the maximum current value in their respective tables. This is crucial after manual data loading or migration to prevent primary key conflicts when new rows are inserted. It includes examples for different schemas (`omar`, `fde-local`).
*   **When to use:**
    *   After a data migration or bulk insert operation to ensure that auto-incrementing primary key sequences are correctly set.
    *   **Important:** Modify the schema names within the script (e.g., `omar`, `fde-local`) to match your target schema.

## Logging

All Python scripts perform logging to a file named `db_transfer.log` in the same directory where the scripts are run. This log file contains information about the script's progress, tables processed, data chunks transferred, and any errors encountered. Review this log for monitoring and troubleshooting.

## Troubleshooting

*   **Connection Errors:**
    *   Ensure PostgreSQL is running and accessible from where you run the script.
    *   Verify that `source_conn_str` and `target_conn_str` are correct (username, password, host, port, database name).
    *   Check firewall rules that might be blocking connections to the database server(s).
    *   Ensure the `psycopg2-binary` package is correctly installed.
*   **Permissions Issues:**
    *   The database user specified in the connection string must have appropriate permissions:
        *   **Source Database:** `SELECT` permissions on all tables and schemas to be migrated. Permissions to read table metadata/definitions.
        *   **Target Database:** `CREATE` permissions for tables and schemas. `INSERT`, `UPDATE`, `DELETE` permissions for data manipulation. `ALTER TABLE` permissions if using the constraint script.
*   **Table Not Found / Schema Issues:**
    *   Double-check `source_schema` and `target_schema` variables.
    *   Ensure table names in `tables_to_exclude` are correct.
*   **Foreign Key Constraint Errors (especially with `transfer_data_script.py`):**
    *   If you encounter FK violations, it might be due to the order of table creation or data insertion. The `transfer_data_with_constraints_script.py` is designed to mitigate these issues by deferring FK creation.
*   **Character Encoding:** Ensure source and target databases use compatible character encodings (ideally UTF-8).
*   **`db_transfer.log`:** Always check this file for detailed error messages.

## Contributing

Contributions are welcome! If you have suggestions for improvements, please open an issue or submit a pull request. (Further details can be added here if this becomes a collaborative project).

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file.
(Assuming a LICENSE file exists. If not, you might want to add one, e.g., MIT License).