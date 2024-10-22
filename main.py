from dotenv import load_dotenv
import os
import json
import time
import binascii
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import datetime

load_dotenv()

# Logging configuration
log_filename = f"{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)


def create_mssql_pool_sqlalchemy(pool_size=5, max_overflow=10, pool_timeout=30):
    logging.info("Creating MSSQL Connection Pool with SQLAlchemy...")
    
    connection_string = (
        f"mssql+pyodbc://{os.getenv('MSSQL_USER')}:{os.getenv('MSSQL_PASSWORD')}@{os.getenv('MSSQL_HOST')}/{os.getenv('MSSQL_DB')}?"
        "driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
    )
    
    try:
        engine = create_engine(
            connection_string,
            pool_size=pool_size,        
            max_overflow=max_overflow,  
            pool_timeout=pool_timeout,  
            pool_pre_ping=True,         
        )
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        Session = sessionmaker(bind=engine)
        return Session, engine
    
    except Exception as e:
        logging.error(f"MSSQL connection pool creation failed: {str(e)}")
        raise  # Hata fırlat




def create_mysql_pool():
    logging.info("Creating MySQL Connection Pool with SQLAlchemy...")
    
    connection_string = (
        f"mysql+mysqldb://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DB')}"
    )
    
    try:
        engine = create_engine(connection_string, pool_size=5)
        
        # Bağlantıyı hemen test etmek için engine ile bir test sorgusu çalıştır
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return engine
    
    except Exception as e:
        logging.error(f"MySQL connection pool creation failed: {str(e)}")
        raise  # Hata fırlat


def get_column_data_type_and_identity(cursor_source, source_schema, source_table, column):
    query = f"""
    SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, 
           COLUMNPROPERTY(object_id('{source_schema}.{source_table}'), '{column}', 'IsIdentity') AS IS_IDENTITY,
           IDENT_SEED('{source_schema}.{source_table}') AS SEED_VALUE,
           IDENT_INCR('{source_schema}.{source_table}') AS INCREMENT_VALUE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{source_table}' AND COLUMN_NAME = '{column}'
    """

    res = cursor_source.execute(text(query))
    result = res.fetchall()

    if result[0]:
        data_type, char_length, numeric_precision, numeric_scale, datetime_precision, is_identity, seed_value, increment_value = result[0]

        # Map MSSQL data types to MySQL equivalents
        if data_type == 'bigint':
            column_type = 'BIGINT'
        elif data_type == 'int':
            column_type = 'INT'
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
            column_type = 'LONGBLOB'  
        elif data_type == 'varbinary':
            column_type = 'LONGBLOB'  
        elif data_type == 'bit':
            column_type = 'TINYINT(1)'
        elif data_type == 'date':
            column_type = 'DATE'
        elif data_type == 'datetime2':
            column_type = 'DATETIME(6)'
        elif data_type == 'datetime':
            column_type = 'DATETIME'
        elif data_type == 'smalldatetime':
            column_type = 'DATETIME'
        elif data_type == 'datetimeoffset':
            column_type = 'VARCHAR(40)'
        elif data_type == 'image':
            column_type = 'LONGBLOB'
        elif data_type == 'money':
            column_type = 'DECIMAL(19,4)'
        elif data_type == 'real':
            column_type = 'FLOAT'
        elif data_type == 'smallmoney':
            column_type = 'DECIMAL(10,4)'
        elif data_type == 'time':
            column_type = 'TIME(6)'
        elif data_type == 'text':
            column_type = 'LONGTEXT'
        elif data_type == 'ntext':
            column_type = 'LONGTEXT'
        elif data_type == 'xml':
            column_type = 'TEXT'
        elif data_type == 'uniqueidentifier':
            column_type = 'CHAR(36)'
        else:
            raise Exception(f"Unknown data type: {data_type}. Unable to map to MySQL.")

        if is_identity and seed_value == 1:
            return f"{column_type} AUTO_INCREMENT", 'PRIMARY KEY', seed_value, increment_value

        return column_type, None, None, None
    else:
        raise Exception(f"Failed to find column type for {column} in table {source_schema}.{source_table}.")


def load_sync_config(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)


def create_target_table(cursor_target, target_table, columns, cursor_source, source_schema, source_table, column_as_is, target_id):
    logging.info(f"Creating target table: {target_table}...")

    columns_definitions = []
    primary_key_column = None
    auto_increment_column = None
    seed_value = None
    increment_value = None

    if column_as_is == 1:
        columns = {col: col for col in columns.keys()}  

    filtered_columns = filter_comparable_columns(cursor_source, source_schema, source_table, columns)

    for source_col, target_col in filtered_columns.items():
        if target_id and source_col == 'id_column':
            target_col = target_id

        data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_schema, source_table, source_col)

        if constraint == 'PRIMARY KEY':
            primary_key_column = target_col
            columns_definitions.append(f"{target_col} {data_type}")
        else:
            columns_definitions.append(f"{target_col} {data_type}")

        if seed is not None and increment is not None:
            auto_increment_column = target_col
            seed_value = seed
            increment_value = increment

    create_query = f"CREATE TABLE {target_table} ({', '.join(columns_definitions)}"

    # Add primary key if applicable
    if primary_key_column:
        create_query += f", PRIMARY KEY ({primary_key_column})"

    create_query += ")"

    logging.info(f"Table creation query: {create_query}")
    cursor_target.execute(create_query)

    if auto_increment_column:
        alter_query = f"ALTER TABLE {target_table} AUTO_INCREMENT = {seed_value}"
        logging.info(f"Setting AUTO_INCREMENT seed: {alter_query}")
        cursor_target.execute(alter_query)


def filter_comparable_columns(cursor_source, source_schema, source_table, columns):
    comparable_columns = {}

    for source_col, target_col in columns.items():
        data_type, _, _, _ = get_column_data_type_and_identity(cursor_source, source_schema, source_table, source_col)

        if data_type not in ('TIME(6)', 'BINARY', 'VARBINARY', 'LONGBLOB'):
            comparable_columns[source_col] = target_col

    return comparable_columns


def sync_data(session_mssql, cursor_target, source_schema, source_table, target_table, columns, id_column, table_as_is,
              column_as_is, target_id=None, conditions=None, query=None):
    logging.debug(f"Starting data synchronization between {source_schema}.{source_table} and {target_table}.")

    # Eğer 'query' varsa, sorguyu çalıştır ve sonuçları doğrudan aktar
    if query:
        logging.debug(f"Executing custom query: {query}")
        result = session_mssql.execute(text(query))
        source_data = result.fetchall()

        # İlk olarak hedef tabloda var olan kayıtları çek
        logging.debug(f"Fetching data from {target_table}.")
        target_query = f"SELECT {target_id} FROM {target_table}"
        result_target = cursor_target.execute(text(target_query))
        target_data = result_target.fetchall()
        target_ids = set([row[0] for row in target_data])

        # source_data'dan gelen ID'leri al
        source_ids = set([row[0] for row in source_data])

        # Silinecek ID'leri belirle (source'da olmayanlar, target'dan silinir)
        ids_to_delete = target_ids - source_ids
        logging.debug(f"IDs to delete: {ids_to_delete}")

        for delete_id in ids_to_delete:
            delete_query = f"DELETE FROM {target_table} WHERE {target_id} = {delete_id}"
            logging.info(f"Executing delete query: {delete_query}")
            cursor_target.execute(text(delete_query))

        for row in source_data:
            id_value = row[0]
            # Tüm sütunları sorguya dahil edin, yalnızca id değil
            check_query = f"SELECT {', '.join(columns.values())} FROM {target_table} WHERE {target_id} = {id_value}"
            logging.debug(f"Check query: {check_query}")
            result = cursor_target.execute(text(check_query))
            target_row = result.fetchone()

            if target_row is None:
                # Eğer target_row boşsa yeni kayıt ekleme işlemi yapılmalı
                columns_list = ', '.join(columns.values())
                values_list = ', '.join(
                    [f"'{str(item)}'" if isinstance(item,
                                                    (str, datetime.datetime, datetime.date, datetime.time)) else str(
                        item)
                     for item in row]
                )

                insert_query = f"INSERT INTO {target_table} ({columns_list}) VALUES ({values_list})"
                logging.info(f"Insert query: {insert_query}")
                cursor_target.execute(text(insert_query))
                continue

            # Güncelleme işlemi için kontrol edilecek kısım
            needs_update = False
            update_data = []

            for i, col in enumerate(columns.keys()):
                value = row[i]
                column_name = columns[col]

                # Hedef satırda ilgili sütunun değerinin var olup olmadığını kontrol et
                if len(target_row) <= i:
                    logging.error(f"Target row does not have enough columns to compare for index {i}")
                    continue

                # Karşılaştırma işlemleri
                if isinstance(value, str):
                    if value != target_row[i]:
                        needs_update = True
                        update_data.append(f"{column_name} = '{value}'")
                elif isinstance(value, datetime.datetime):
                    # Tarih karşılaştırmasını yalnızca Yıl-Ay-Gün Saat:Dakika:Saniye ile yapıyoruz, mikrosaniyeleri atlıyoruz
                    source_value_str = value.strftime('%Y-%m-%d %H:%M:%S')
                    target_value_str = target_row[i].strftime('%Y-%m-%d %H:%M:%S')
                    if source_value_str != target_value_str:
                        needs_update = True
                        update_data.append(f"{column_name} = '{source_value_str}'")
                else:
                    if value != target_row[i]:
                        needs_update = True
                        update_data.append(f"{column_name} = {value}")

            # needs_update true ise update yap
            if needs_update:
                update_query = f"UPDATE {target_table} SET {', '.join(update_data)} WHERE {target_id} = {id_value}"
                logging.info(f"Update query: {update_query}")
                cursor_target.execute(text(update_query))

        logging.debug(f"Data synchronization between {source_schema}.{source_table} and {target_table} completed.")
        return  # 'query' varsa başka işlem yapılmasına gerek yok

    # Eğer 'query' yoksa, normal eşleştirme ve kolon kontrolüne geç
    else:
        if table_as_is == 1:
            target_table = source_table

        if column_as_is == 1:
            columns = {col: col for col in get_source_columns(session_mssql, source_schema, source_table)}
            target_id = id_column
        else:
            if target_id:
                columns[id_column] = target_id

        logging.debug(f"Fetching data from {source_schema}.{source_table}.")

        source_query = f"SELECT {id_column} FROM {source_schema}.{source_table}"
        if conditions:
            source_query += f" WHERE {conditions}"

        result = session_mssql.execute(text(source_query))
        source_data = result.fetchall()
        source_ids = set([row[0] for row in source_data])

        # Target tablodaki veriyi çek
        logging.debug(f"Fetching data from {target_table}.")
        target_query = f"SELECT {target_id} FROM {target_table}"
        result = cursor_target.execute(text(target_query))
        target_data = result.fetchall()
        target_ids = set([row[0] for row in target_data])

        # Silinecek ID'leri belirle
        ids_to_delete = target_ids - source_ids
        logging.debug(f"IDs to delete: {ids_to_delete}")

        for delete_id in ids_to_delete:
            delete_query = f"DELETE FROM {target_table} WHERE {target_id} = {delete_id}"
            logging.info(f"Executing delete query: {delete_query}")
            cursor_target.execute(text(delete_query))

        comparable_columns = filter_comparable_columns(session_mssql, source_schema, source_table, columns)

        logging.debug(f"Fetching comparable data from {source_schema}.{source_table}.")
        source_query = f"SELECT {id_column}, {', '.join([col for col in comparable_columns.keys() if col != id_column])} FROM {source_schema}.{source_table}"
        if conditions:
            source_query += f" WHERE {conditions}"

        result = session_mssql.execute(text(source_query))
        source_data = result.fetchall()

        for row in source_data:
            row_data = {}
            column_order = list(comparable_columns.keys())

            for i, col in enumerate(column_order):
                if i < len(row):
                    row_data[col] = row[i]

            id_value = row[0]

            check_query = f"SELECT {', '.join(comparable_columns.values())} FROM {target_table} WHERE {target_id} = {id_value}"
            logging.debug(f"Check query: {check_query}")
            result = cursor_target.execute(text(check_query))
            target_row = result.fetchone()

            if target_row:
                # Verilerin farklı olup olmadığını kontrol et
                needs_update = False
                update_data = []

                for idx, (source_col, target_col) in enumerate(comparable_columns.items()):
                    source_value = row_data[source_col]
                    target_value = target_row[idx]

                    if source_col in ['CREATE_DATE', 'MODIFY_DATE', 'created_at', 'modified_at']:
                        if isinstance(source_value, datetime.datetime) and isinstance(target_value, datetime.datetime):
                            source_value_str = source_value.strftime('%Y-%m-%d %H:%M:%S')
                            target_value_str = target_value.strftime('%Y-%m-%d %H:%M:%S')
                            if source_value_str != target_value_str:
                                needs_update = True
                                update_data.append(f"{target_col} = '{source_value_str}'")
                        continue  # Tarih alanını bu koşullar altında geç

                    # Diğer alanlar için normal karşılaştırma
                    if source_value != target_value:
                        needs_update = True
                        if isinstance(source_value, str):
                            update_data.append(f"{target_col} = '{source_value}'")
                        elif isinstance(source_value, datetime.datetime):
                            update_data.append(f"{target_col} = '{source_value.strftime('%Y-%m-%d %H:%M:%S.%f')}'")
                        else:
                            update_data.append(f"{target_col} = {source_value}")

                # Güncelleme gerekiyorsa UPDATE sorgusunu çalıştır
                if needs_update:
                    update_query = f"UPDATE {target_table} SET {', '.join(update_data)} WHERE {target_id} = {id_value}"
                    logging.info(f"Update query: {update_query}")
                    cursor_target.execute(text(update_query))
            else:
                # Yeni bir kayıt ekle
                columns_list = ', '.join(list(comparable_columns.values()))
                values_list = ', '.join(
                    [
                        f"'{binascii.hexlify(row_data[col]).decode()}'" if isinstance(row_data[col], bytes) else
                        f"'{row_data[col].strftime('%Y-%m-%d %H:%M:%S.%f')}'" if isinstance(row_data[col],
                                                                                            datetime.datetime) else
                        f"'{row_data[col]}'" if isinstance(row_data[col], str) else
                        str(row_data[col])
                        for col in row_data.keys()
                    ]
                )

                insert_query = f"INSERT INTO {target_table} ({columns_list}) VALUES ({values_list})"
                logging.info(f"Insert query: {insert_query}")
                cursor_target.execute(text(insert_query))

        logging.debug(f"Data synchronization between {source_schema}.{source_table} and {target_table} completed.")


def sync(json_file, mssql_pool, mysql_pool):
    session_mssql = mssql_pool()
    conn_mysql = mysql_pool.connect()

    try:
        sync_config = load_sync_config(json_file)

        for table in sync_config['tables']:
            source_table = table.get('source_table')
            source_schema = table.get('source_schema')
            table_as_is = table.get('table_as_is', 0)
            column_as_is = table.get('column_as_is', 0)
            target_id = table.get('target_id', None)
            conditions = table.get('conditions', None)
            query = table.get('query', None)

            if table_as_is == 1:
                target_table = source_table
            else:
                if 'target_table' not in table:
                    raise Exception(f"target_table must be specified in JSON because table_as_is = 0")
                target_table = table['target_table']

            if column_as_is == 1:
                columns = get_source_columns(session_mssql, source_schema, source_table)
                columns = {col: col for col in columns}
            else:
                if 'columns' not in table:
                    raise Exception(f"columns must be specified in JSON because column_as_is = 0")
                columns = table['columns']

            id_column = table['id_column']

            sync_data(session_mssql, conn_mysql, source_schema, source_table, target_table, columns, id_column,
                      table_as_is, column_as_is, target_id=target_id, conditions=conditions, query=query)

        session_mssql.commit()
        conn_mysql.commit()

    finally:
        session_mssql.close()
        conn_mysql.close()


def check_and_create_columns(cursor_source, cursor_target, source_schema, source_table, target_table, columns, table_as_is, column_as_is, target_id):
    logging.debug(f"Checking and creating columns between {source_schema}.{source_table} and {target_table}.")

    if table_as_is == 1:
        target_table = source_table

    if column_as_is == 1:
        columns = {col: col for col in get_source_columns(cursor_source, source_schema, source_table)}


    cursor_target.execute(f"SHOW TABLES LIKE '{target_table}'")
    if cursor_target.fetchall() is None:
        create_target_table(cursor_target, target_table, columns, cursor_source, source_schema, source_table, column_as_is, target_id)
        return

    source_columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{source_table}'"
    cursor_source.execute(source_columns_query)
    source_columns = [row[0] for row in cursor_source.fetchall()]

    target_columns_query = f"SHOW COLUMNS FROM {target_table}"
    cursor_target.execute(target_columns_query)
    target_columns = [row[0] for row in cursor_target.fetchall()]

    for source_col, target_col in columns.items():
        if source_col not in source_columns:
            logging.error(f"Missing column {source_col} in source table {source_schema}.{source_table}")
            raise Exception(f"Source table {source_schema}.{source_table} missing column {source_col}")
        if target_col not in target_columns:
            data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_schema, source_table, source_col)
            alter_query = f"ALTER TABLE {target_table} ADD COLUMN {target_col} {data_type}"
            logging.info(f"Adding column to target table: {alter_query}")
            cursor_target.execute(alter_query)

    logging.debug(f"Column check and creation between {source_schema}.{source_table} and {target_table} completed.")

def get_source_columns(cursor_source, source_schema, source_table):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{source_table}'"
    cursor_source.execute(query)
    source_columns = [row[0] for row in cursor_source.fetchall()]
    return source_columns



if __name__ == "__main__":
    try:
        logging.info("#################################################################################################################")
        logging.info("########################## PosPro Portal / DB Synchronization Service Started ###############################")
        logging.info("#################################################################################################################")
        
        sync_config = load_sync_config('conf.json')
        frequency = int(os.getenv('frequency', '60'))

        mssql_pool = None
        mysql_pool = None


        while True:
            try:
                logging.info("Connecting to MySQL and MSSQL databases...")
                
                try:
                    mssql_pool, mssql_engine = create_mssql_pool_sqlalchemy()
                    logging.info("MSSQL connection is successful.")
                except Exception as mssql_error:
                    logging.error(f"MSSQL connection failed: {str(mssql_error)}")
                    raise mssql_error 

                try:
                    mysql_pool = create_mysql_pool()
                    logging.info("MySQL connection is successful.")
                except Exception as mysql_error:
                    logging.error(f"MySQL connection failed: {str(mysql_error)}")
                    raise mysql_error  
                
                logging.info("Connections to both MySQL and MSSQL databases are successful.")
                break  

            except Exception as conn_error:
                logging.error(f"Connection Error, trying again in 1 minute: {str(conn_error)}")
                time.sleep(60)  

        while True:
            try:
                sync('conf.json', mssql_pool, mysql_pool)
            except Exception as sync_error:
                logging.error(f"Error during synchronization: {str(sync_error)}")
            time.sleep(frequency) 

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
