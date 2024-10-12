import pyodbc
from mysql.connector import pooling
from dotenv import load_dotenv
import os
import json
import time
import binascii
import logging

from queue import Queue
import datetime

load_dotenv()

# Logging configuration
log_filename = f"{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)


# Create MSSQL connection pool
def create_mssql_pool(pool_size=5):
    logging.info("Creating MSSQL Connection Pool...")
    pool = Queue(maxsize=pool_size)
    for _ in range(pool_size):
        conn = pyodbc.connect(
            f"DRIVER={{/opt/homebrew/lib/libmsodbcsql.17.dylib}};"
            f"SERVER={os.getenv('MSSQL_SERVER')};"
            f"DATABASE={os.getenv('MSSQL_DB')};"
            f"UID={os.getenv('MSSQL_USER')};"
            f"PWD={os.getenv('MSSQL_PASSWORD')}"
        )
        pool.put(conn)
    return pool


# Create MySQL connection pool
def create_mysql_pool():
    logging.info("Creating MySQL Connection Pool...")
    pool = pooling.MySQLConnectionPool(
        pool_name="mysql_pool",
        pool_size=5,
        host=os.getenv('MYSQL_HOST'),
        database=os.getenv('MYSQL_DB'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD')
    )
    return pool

def get_column_data_type_and_identity(cursor_source, source_table, column):
    query = f"""
    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, 
           COLUMNPROPERTY(object_id('{source_table}'), '{column}', 'IsIdentity') AS IS_IDENTITY,
           IDENT_SEED('{source_table}') AS SEED_VALUE,
           IDENT_INCR('{source_table}') AS INCREMENT_VALUE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{source_table}' AND COLUMN_NAME = '{column}'
    """
    cursor_source.execute(query)
    result = cursor_source.fetchone()

    if result:
        data_type, char_length, numeric_precision, numeric_scale, datetime_precision, is_identity, seed_value, increment_value = result

        # Map MSSQL data types to MySQL equivalents
        if data_type == 'bigint':
            column_type = 'BIGINT'
        elif data_type == 'smallint':
            column_type = 'SMALLINT'
        elif data_type == 'tinyint':
            column_type = 'TINYINT'
        elif data_type == 'float':
            column_type = 'FLOAT'
        elif data_type in ('decimal', 'numeric'):
            column_type = f"DECIMAL({numeric_precision},{numeric_scale})"
        elif data_type in ('nvarchar', 'varchar'):
            if char_length == -1:  # MSSQL's MAX equivalent
                column_type = 'TEXT'
            else:
                column_type = f"VARCHAR({char_length})"
        elif data_type == 'char':
            column_type = f"CHAR({char_length})"
        elif data_type == 'binary':
            column_type = 'LONGBLOB'  # Using LONGBLOB for large binary data
        elif data_type == 'varbinary':
            column_type = 'LONGBLOB'  # Using LONGBLOB for large binary data
        elif data_type == 'bit':
            column_type = 'TINYINT(1)'  # Use TINYINT(1) for boolean or bit in MySQL
        elif data_type == 'date':
            column_type = 'DATE'
        elif data_type == 'datetime2':
            column_type = 'DATETIME(6)'  # Use DATETIME with 6 precision in MySQL
        elif data_type == 'datetime':
            column_type = 'DATETIME'
        elif data_type == 'smalldatetime':
            column_type = 'DATETIME'  # No small-datetime equivalent in MySQL, use DATETIME
        elif data_type == 'datetimeoffset':
            column_type = 'VARCHAR(40)'  # Store datetimeoffset as VARCHAR
        elif data_type == 'image':
            column_type = 'LONGBLOB'  # Use LONGBLOB for image data type
        elif data_type == 'money':
            column_type = 'DECIMAL(19,4)'  # Map money to DECIMAL(19,4)
        elif data_type == 'real':
            column_type = 'FLOAT'  # Map real to float in MySQL
        elif data_type == 'smallmoney':
            column_type = 'DECIMAL(10,4)'  # Map smallmoney to DECIMAL(10,4)
        elif data_type == 'time':
            column_type = 'TIME(6)'  # Use TIME with 6 precision in MySQL
        elif data_type == 'text':
            column_type = 'LONGTEXT'  # Map text to LONGTEXT in MySQL
        elif data_type == 'ntext':
            column_type = 'LONGTEXT'  # Map ntext to LONGTEXT in MySQL
        elif data_type == 'xml':
            column_type = 'TEXT'  # XML can be stored as TEXT in MySQL
        elif data_type == 'uniqueidentifier':
            column_type = 'CHAR(36)'  # Use CHAR(36) for uniqueidentifier
        else:
            raise Exception(f"Unknown data type: {data_type}. Unable to map to MySQL.")

        # If the column is an identity, make it AUTO_INCREMENT in MySQL
        if is_identity and seed_value == 1:
            return f"{column_type} AUTO_INCREMENT", 'PRIMARY KEY', seed_value, increment_value

        # Default return if it's not an identity column
        return column_type, None, None, None
    else:
        raise Exception(f"Failed to find column type for {column} in table {source_table}.")


def load_sync_config(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)


# Create target table if it doesn't exist
def create_target_table(cursor_target, target_table, columns, cursor_source, source_table, column_as_is, target_id):
    logging.info(f"Creating target table: {target_table}...")

    columns_definitions = []
    primary_key_column = None  # Store primary key column
    auto_increment_column = None  # Store auto_increment column
    seed_value = None
    increment_value = None

    # Use source column names if column_as_is is 1
    if column_as_is == 1:
        columns = {col: col for col in columns.keys()}  # Use source column names

    # Filter columns to exclude certain data types
    filtered_columns = filter_comparable_columns(cursor_source, source_table, columns)

    for source_col, target_col in filtered_columns.items():
        # If target_id is provided, map the source id_column to the target_id
        if target_id and source_col == 'id_column':
            target_col = target_id

        # Get data type and constraint for each column
        data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_table, source_col)

        # If the column is primary key, add PRIMARY KEY and AUTO_INCREMENT
        if constraint == 'PRIMARY KEY':
            primary_key_column = target_col
            columns_definitions.append(f"{target_col} {data_type}")
        else:
            columns_definitions.append(f"{target_col} {data_type}")

        # Store identity column information for AUTO_INCREMENT
        if seed is not None and increment is not None:
            auto_increment_column = target_col
            seed_value = seed
            increment_value = increment

    # Create the table with the gathered column definitions
    create_query = f"CREATE TABLE {target_table} ({', '.join(columns_definitions)}"

    # Add primary key if applicable
    if primary_key_column:
        create_query += f", PRIMARY KEY ({primary_key_column})"

    create_query += ")"  # Close the table creation query

    logging.info(f"Table creation query: {create_query}")
    cursor_target.execute(create_query)

    # Set AUTO_INCREMENT seed if applicable
    if auto_increment_column:
        alter_query = f"ALTER TABLE {target_table} AUTO_INCREMENT = {seed_value}"
        logging.info(f"Setting AUTO_INCREMENT seed: {alter_query}")
        cursor_target.execute(alter_query)


# Filter columns to exclude certain data types for comparison
def filter_comparable_columns(cursor_source, source_table, columns):
    comparable_columns = {}

    for source_col, target_col in columns.items():
        # Get column data type
        data_type, _, _, _ = get_column_data_type_and_identity(cursor_source, source_table, source_col)

        # Exclude certain data types from comparison
        if data_type not in ('TIME(6)', 'BINARY', 'VARBINARY', 'LONGBLOB'):
            comparable_columns[source_col] = target_col

    return comparable_columns


def sync_data(cursor_source, cursor_target, source_table, target_table, columns, id_column, table_as_is, column_as_is,
              target_id=None, conditions=None):
    logging.debug(f"Starting data synchronization between {source_table} and {target_table}.")

    # If table_as_is = 1, align target_table with source_table
    if table_as_is == 1:
        target_table = source_table

    # If column_as_is = 1, use source table column names as is
    if column_as_is == 1:
        columns = {col: col for col in get_source_columns(cursor_source, source_table)}  # Use source table columns
        target_id = id_column  # Align target_id with source table ID
    else:
        # If column_as_is = 0, align columns based on target_id
        if target_id:
            columns[id_column] = target_id

    logging.debug(f"Fetching data from {source_table}.")

    # Construct the base query for fetching data from MSSQL
    source_query = f"SELECT {id_column} FROM {source_table}"

    # If conditions are provided, add them to the query
    if conditions:
        source_query += f" WHERE {conditions}"

    logging.debug(f"Source query with conditions: {source_query}")
    cursor_source.execute(source_query)
    source_data = cursor_source.fetchall()
    source_ids = set([row[0] for row in source_data])  # Get IDs from source

    logging.debug(f"Fetching data from {target_table}.")
    # Fetch data from MySQL
    target_query = f"SELECT {target_id} FROM {target_table}"
    cursor_target.execute(target_query)
    target_data = cursor_target.fetchall()
    target_ids = set([row[0] for row in target_data])  # Get IDs from target

    # Identify IDs that are in MySQL but not in MSSQL (to be deleted)
    ids_to_delete = target_ids - source_ids

    logging.debug(f"IDs to delete: {ids_to_delete}")
    # Delete those IDs from MySQL
    for delete_id in ids_to_delete:
        delete_query = f"DELETE FROM {target_table} WHERE {target_id} = {delete_id}"
        logging.info(f"Executing delete query: {delete_query}")
        cursor_target.execute(delete_query)

    # Filter columns for comparison (exclude time, binary, and varbinary columns)
    comparable_columns = filter_comparable_columns(cursor_source, source_table, columns)

    logging.debug(f"Fetching comparable data from {source_table}.")
    # Fetch data from MSSQL excluding non-comparable columns
    source_query = f"SELECT {id_column}, {', '.join([col for col in comparable_columns.keys() if col != id_column])} FROM {source_table}"

    if conditions:
        source_query += f" WHERE {conditions}"

    logging.debug(f"Source data query: {source_query}")
    cursor_source.execute(source_query)
    source_data = cursor_source.fetchall()

    for row in source_data:
        row_data = {}
        column_order = list(comparable_columns.keys())  # Column order in MSSQL

        # Map row data to corresponding columns
        for i, col in enumerate(column_order):
            if i < len(row):
                row_data[col] = row[i]

        id_value = row[0]  # Get the ID from MSSQL

        # Check if the record exists in MySQL
        check_query = f"SELECT {', '.join(comparable_columns.values())} FROM {target_table} WHERE {target_id} = {id_value}"
        logging.debug(f"Check query: {check_query}")
        cursor_target.execute(check_query)
        target_row = cursor_target.fetchone()

        if target_row:
            # If record exists, update it if necessary
            update_data = []
            target_row_list = list(target_row)
            for source_col, target_col in comparable_columns.items():
                if row_data[source_col] != target_row_list[column_order.index(source_col)]:
                    update_data.append(f"{target_col} = {row_data[source_col]}")

            if update_data:
                update_query = f"UPDATE {target_table} SET {', '.join(update_data)} WHERE {target_id} = {id_value}"
                logging.info(f"Update query: {update_query}")
                cursor_target.execute(update_query)
        else:
            # If record does not exist, insert it
            filtered_columns = filter_comparable_columns(cursor_source, source_table, columns)
            columns_list = ', '.join(list(filtered_columns.values()))
            values_list = ', '.join(
                [
                    f"'{binascii.hexlify(row_data[col]).decode()}'" if isinstance(row_data[col], bytes) else
                    f"'{row_data[col].strftime('%Y-%m-%d %H:%M:%S.%f')}'" if isinstance(row_data[col],
                                                                                        datetime.datetime) else
                    f"'{row_data[col].strftime('%Y-%m-%d')}'" if isinstance(row_data[col],
                                                                            datetime.date) and not isinstance(
                        row_data[col], datetime.datetime) else
                    f"'{row_data[col].strftime('%H:%M:%S.%f')}'" if isinstance(row_data[col], datetime.time) else
                    f"'{row_data[col]}'" if isinstance(row_data[col], str) else
                    str(row_data[col]) if isinstance(row_data[col], (int, float)) else
                    str(row_data[col])
                    for col in row_data.keys()
                ]
            )

            insert_query = f"INSERT INTO {target_table} ({columns_list}) VALUES ({values_list})"
            logging.info(f"Insert query: {insert_query}")
            cursor_target.execute(insert_query)

    logging.debug(f"Data synchronization between {source_table} and {target_table} completed.")


def sync(json_file, mssql_pool, mysql_pool):
    conn_mssql = mssql_pool.get()
    conn_mysql = mysql_pool.get_connection()
    cursor_mssql = conn_mssql.cursor()
    cursor_mysql = conn_mysql.cursor()

    sync_config = load_sync_config(json_file)

    for table in sync_config['tables']:
        source_table = table['source_table']
        table_as_is = table.get('table_as_is', 0)
        column_as_is = table.get('column_as_is', 0)
        target_id = table.get('target_id', None)

        # Check if conditions are present in the JSON
        conditions = table.get('conditions', None)  # Get conditions if they exist

        if table_as_is == 1:
            target_table = source_table
        else:
            if 'target_table' not in table:
                raise Exception(f"target_table must be specified in JSON because table_as_is = 0")
            target_table = table['target_table']

        if column_as_is == 1:
            columns = get_source_columns(cursor_mssql, source_table)
            columns = {col: col for col in columns}
        else:
            if 'columns' not in table:
                raise Exception(f"columns must be specified in JSON because column_as_is = 0")
            columns = table['columns']

        id_column = table['id_column']

        check_and_create_columns(cursor_mssql, cursor_mysql, source_table, target_table, columns, table_as_is,
                                 column_as_is, target_id)

        # Pass the conditions to sync_data if they exist
        sync_data(cursor_mssql, cursor_mysql, source_table, target_table, columns, id_column, table_as_is, column_as_is,
                  target_id=target_id, conditions=conditions)

    conn_mssql.commit()
    conn_mysql.commit()
    cursor_mssql.close()
    cursor_mysql.close()
    conn_mysql.close()
    mssql_pool.put(conn_mssql)


def check_and_create_columns(cursor_source, cursor_target, source_table, target_table, columns, table_as_is, column_as_is, target_id):
    logging.debug(f"Checking and creating columns between {source_table} and {target_table}.")

    if table_as_is == 1:
        target_table = source_table

    if column_as_is == 1:
        columns = {col: col for col in get_source_columns(cursor_source, source_table)}

    cursor_target.execute(f"SHOW TABLES LIKE '{target_table}'")
    if cursor_target.fetchone() is None:
        create_target_table(cursor_target, target_table, columns, cursor_source, source_table, column_as_is, target_id)
        return

    source_columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{source_table}'"
    cursor_source.execute(source_columns_query)
    source_columns = [row[0] for row in cursor_source.fetchall()]

    target_columns_query = f"SHOW COLUMNS FROM {target_table}"
    cursor_target.execute(target_columns_query)
    target_columns = [row[0] for row in cursor_target.fetchall()]

    for source_col, target_col in columns.items():
        if source_col not in source_columns:
            logging.error(f"Missing column {source_col} in source table {source_table}")
            raise Exception(f"Source table {source_table} missing column {source_col}")
        if target_col not in target_columns:
            data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_table, source_col)
            alter_query = f"ALTER TABLE {target_table} ADD COLUMN {target_col} {data_type}"
            logging.info(f"Adding column to target table: {alter_query}")
            cursor_target.execute(alter_query)

    logging.debug(f"Column check and creation between {source_table} and {target_table} completed.")

def get_source_columns(cursor_source, source_table):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{source_table}'"
    cursor_source.execute(query)
    source_columns = [row[0] for row in cursor_source.fetchall()]
    return source_columns


# Sürekli sync işlemi
if __name__ == "__main__":
    try:
        logging.info("dbsync service started.")
        mssql_pool = create_mssql_pool()
        mysql_pool = create_mysql_pool()
        sync_config = load_sync_config('conf.json')
        frequency = sync_config['frequency']

        while True:
            sync('conf.json', mssql_pool, mysql_pool)
            time.sleep(frequency)

    except Exception as e:
        logging.error(f"Hata: {str(e)}")
