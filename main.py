import pyodbc
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
import os
import json
import time
import logging
from datetime import datetime
from queue import Queue

# .env dosyasını yükle
load_dotenv()

# Loglama ayarları
log_filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)


# MSSQL connection pool oluşturma
def create_mssql_pool(pool_size=5):
    logging.debug("Creating MSSQL Connection Pool...")
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


# MySQL connection pool oluşturma
def create_mysql_pool():
    logging.debug("Creating MySQL Connection Pool...")
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

        # MSSQL veri tiplerini MySQL'e dönüştürüyoruz
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
            if char_length == -1:  # MSSQL'deki MAX karşılığı
                column_type = 'TEXT'
            else:
                column_type = f"VARCHAR({char_length})"
        elif data_type == 'char':
            column_type = f"CHAR({char_length})"
        elif data_type == 'binary':
            column_type = f"BINARY({char_length})"
        elif data_type == 'varbinary':
            if char_length == -1:  # MSSQL'deki MAX karşılığı
                column_type = 'LONGBLOB'
            else:
                column_type = f"VARBINARY({char_length})"
        elif data_type == 'bit':
            column_type = 'TINYINT(1)'  # MySQL'de boolean ya da bit yerine TINYINT(1) kullanılır
        elif data_type == 'date':
            column_type = 'DATE'
        elif data_type == 'datetime2':
            column_type = 'DATETIME(6)'  # MySQL'de datetime hassasiyeti 6 basamak
        elif data_type == 'datetime':
            column_type = 'DATETIME'
        elif data_type == 'smalldatetime':
            column_type = 'DATETIME'  # MySQL'de smalldatetime yok, datetime kullanılır
        elif data_type == 'datetimeoffset':
            column_type = 'VARCHAR(40)'  # datetimeoffset tipi VARCHAR olarak saklanacak
        elif data_type == 'image':
            column_type = 'LONGBLOB'  # MSSQL'deki image için MySQL'de LONGBLOB kullanılır
        elif data_type == 'money':
            column_type = 'DECIMAL(19,4)'  # MySQL'de money karşılığı decimal(19,4) kullanılır
        elif data_type == 'real':
            column_type = 'FLOAT'  # real tipi MySQL'de float olarak kullanılır
        elif data_type == 'smallmoney':
            column_type = 'DECIMAL(10,4)'  # smallmoney karşılığı decimal(10,4)
        elif data_type == 'time':
            column_type = 'TIME(6)'  # time hassasiyeti MySQL'de 6 basamak
        elif data_type == 'text':
            column_type = 'LONGTEXT'  # MySQL'de text karşılığı LONGTEXT kullanılır
        elif data_type == 'ntext':
            column_type = 'LONGTEXT'  # MySQL'de ntext karşılığı da LONGTEXT'tir
        elif data_type == 'xml':
            column_type = 'TEXT'  # MySQL'de xml için TEXT kullanılabilir
        elif data_type == 'uniqueidentifier':
            column_type = 'CHAR(36)'  # MySQL'de uniqueidentifier için CHAR(36) kullanılır
        else:
            raise Exception(f"Bilinmeyen veri tipi: {data_type} için dönüşüm yapılamadı.")

        # Eğer identity kolonuysa, targetta hem PRIMARY KEY hem de AUTO_INCREMENT olacak
        if is_identity and seed_value == 1:
            logging.debug(f"Identity column found: {column} with seed {seed_value} and increment {increment_value}")
            return f"{column_type} AUTO_INCREMENT", 'PRIMARY KEY', seed_value, increment_value

        # Varsayılan veri tipi dönüşü (identity değilse seed ve increment değerleri None)
        return column_type, None, None, None
    else:
        raise Exception(f"Source'da {source_table} tablosunda {column} kolonunun tipi bulunamadı.")


def load_sync_config(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)





# Target üzerinde tablo oluşturma
def create_target_table(cursor_target, target_table, columns, cursor_source, source_table, column_as_is, target_id):
    logging.debug(f"{target_table} tablosu bulunamadı, oluşturuluyor.")

    columns_definitions = []
    primary_key_column = None  # Primary key olan kolonu saklayacağız
    auto_increment_column = None  # AUTO_INCREMENT olacak kolon
    seed_value = None
    increment_value = None

    # Eğer column_as_is = 1 ise, kolon isimlerini kaynak tablodaki gibi kullanacağız
    if column_as_is == 1:
        columns = {col: col for col in columns.keys()}  # Kaynaktaki isimlerle aynı olacak şekilde ayarlıyoruz

    for source_col, target_col in columns.items():
        # Eğer target_id varsa, bu target_col target_id olarak atanacak
        if target_id and source_col == 'id_column':
            target_col = target_id

        data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_table, source_col)

        # Eğer constraint 'PRIMARY KEY' ise, targetta PRIMARY KEY ve AUTO_INCREMENT olarak ayarlayacağız
        if constraint == 'PRIMARY KEY':
            primary_key_column = target_col
            columns_definitions.append(f"{target_col} {data_type}")
        else:
            columns_definitions.append(f"{target_col} {data_type}")

        # Eğer identity auto_increment ise, seed ve increment değerlerini sakla
        if seed is not None and increment is not None:
            auto_increment_column = target_col
            seed_value = seed
            increment_value = increment

    # Tabloyu oluştur
    create_query = f"CREATE TABLE {target_table} ({', '.join(columns_definitions)}"

    # Eğer primary key varsa, onu da ekleyelim
    if primary_key_column:
        create_query += f", PRIMARY KEY ({primary_key_column})"

    create_query += ")"  # Fazladan virgülden kaçınmak için

    logging.debug(f"Create table query: {create_query}")
    cursor_target.execute(create_query)

    # AUTO_INCREMENT varsa seed ve increment değerlerini ayarla
    if auto_increment_column:
        alter_query = f"ALTER TABLE {target_table} AUTO_INCREMENT = {seed_value}"
        logging.debug(f"Set AUTO_INCREMENT seed: {alter_query}")
        cursor_target.execute(alter_query)



def sync_data(cursor_source, cursor_target, source_table, target_table, columns, id_column, table_as_is, column_as_is, target_id=None):
    logging.debug(f"{source_table} ve {target_table} tabloları arasında veri senkronizasyonu başlatıldı.")

    # Eğer table_as_is = 1 ise, target_table'ı source_table ile eşleştiriyoruz
    if table_as_is == 1:
        target_table = source_table

    # Eğer column_as_is = 1 ise, kolon isimlerini source'dan alıyoruz
    if column_as_is == 1:
        columns = {col: col for col in get_source_columns(cursor_source, source_table)}

    # source tablodaki orijinal id_column (örneğin 'ID' SQL Server'da)
    real_id_column = id_column  # SQL Server'daki orijinal kolon ismi ('ID' gibi)

    # target_id varsa, hedef tabloya göre id_column'u ayarlıyoruz, ama source_table'da real_id_column kullanılmalı
    if target_id:
        columns[real_id_column] = target_id  # Hedef tabloda target_id olacak, kaynak tabloyla eşleştirilecek
        id_column = target_id  # Hedef tablo için target_id kullanılacak

    # Kaynak tablo (source_table) için orijinal id_column'u (örneğin SQL Server'da 'ID') sorguluyoruz
    source_query = f"SELECT {', '.join([real_id_column] + list(columns.keys()))} FROM {source_table}"
    print("source_query: ", source_query)
    cursor_source.execute(source_query)
    source_data = cursor_source.fetchall()

    for row in source_data:
        row_data = {col: row[i] for i, col in enumerate([real_id_column] + list(columns.keys()))}
        id_value = row_data.pop(real_id_column)  # SQL Server'daki ID değerini alıyoruz
        print("id_value: ", id_value)

        # Veriyi MySQL için uygun formatta hazırlamak
        for col, val in row_data.items():
            if isinstance(val, bytes):  # Eğer binary veri varsa, hex formatına çevir
                row_data[col] = f"X'{val.hex()}'"
            elif isinstance(val, bool):  # Boolean veri tipini dönüştür
                row_data[col] = '1' if val else '0'
            else:
                row_data[col] = f"'{val}'"

        # Hedef tabloda target_id'yi (ya da id_column) kullanarak kontrol yapıyoruz
        check_query = f"SELECT {id_column} FROM {target_table} WHERE {id_column} = {id_value}"
        print("check_query: ", check_query)
        cursor_target.execute(check_query)
        target_row = cursor_target.fetchone()

        if target_row:
            update_data = []
            for source_col, target_col in columns.items():
                if row_data[source_col] != target_row.get(target_col):
                    update_data.append(f"{target_col} = {row_data[source_col]}")

            if update_data:
                update_query = f"UPDATE {target_table} SET {', '.join(update_data)} WHERE {id_column} = {id_value}"
                logging.debug(f"Update query: {update_query}")
                cursor_target.execute(update_query)
        else:
            columns_list = ', '.join(columns.values())  # Target tarafındaki kolon isimleri
            values_list = ', '.join([row_data[col] for col in columns.keys()])
            insert_query = f"INSERT INTO {target_table} ({columns_list}) VALUES ({values_list})"
            logging.debug(f"Insert query: {insert_query}")
            cursor_target.execute(insert_query)





def sync(json_file, mssql_pool, mysql_pool):
    conn_mssql = mssql_pool.get()
    conn_mysql = mysql_pool.get_connection()
    cursor_mssql = conn_mssql.cursor()
    cursor_mysql = conn_mysql.cursor()

    sync_config = load_sync_config(json_file)

    for table in sync_config['tables']:
        source_table = table['source_table']
        table_as_is = table.get('table_as_is', 0)  # table_as_is değeri JSON'dan alınıyor
        column_as_is = table.get('column_as_is', 0)  # column_as_is değeri JSON'dan alınıyor
        target_id = table.get('target_id', None)  # target_id kontrolü

        # table_as_is = 1 ise target_table yoksa hata vermemeli
        if table_as_is == 1:
            target_table = source_table  # source_table target_table olarak kullanılacak
        else:
            # table_as_is = 0 ise target_table JSON'da mevcut olmalı
            if 'target_table' not in table:
                raise Exception(f"JSON dosyasında target_table bulunmalı çünkü table_as_is = 0")
            target_table = table['target_table']

        # column_as_is = 1 ise columns yoksa hata vermemeli
        if column_as_is == 1:
            columns = get_source_columns(cursor_mssql, source_table)  # MSSQL'deki kolon isimlerini alıyoruz
            columns = {col: col for col in columns}  # Kolon eşleştirmesi birebir olacak
        else:
            # column_as_is = 0 ise columns JSON'da mevcut olmalı ve match etmeli
            if 'columns' not in table:
                raise Exception(f"JSON dosyasında columns bulunmalı çünkü column_as_is = 0")
            columns = table['columns']

        id_column = table['id_column']

        # target_id varsa, identity kolonu target_id'de belirtilen isimde olacak
        if target_id:
            columns[id_column] = target_id
            id_column = target_id  # Hedef tabloya göre id_column ayarlanıyor

        # check_and_create_columns fonksiyonuna table_as_is, column_as_is ve target_id ekleniyor
        check_and_create_columns(cursor_mssql, cursor_mysql, source_table, target_table, columns, table_as_is, column_as_is, target_id)

        # sync_data fonksiyonuna table_as_is ve column_as_is ekleniyor
        sync_data(cursor_mssql, cursor_mysql, source_table, target_table, columns, id_column, table_as_is, column_as_is)

    conn_mssql.commit()
    conn_mysql.commit()
    cursor_mssql.close()
    cursor_mysql.close()
    mssql_pool.put(conn_mssql)

# Tablo ve kolonları kontrol et
def check_and_create_columns(cursor_source, cursor_target, source_table, target_table, columns, table_as_is, column_as_is, target_id):
    logging.debug(f"{source_table} ve {target_table} tabloları için kolon kontrolü başlatıldı.")

    # Eğer table_as_is = 1 ise, target_table'ı source_table olarak ayarla
    if table_as_is == 1:
        target_table = source_table

    # Eğer column_as_is = 1 ise, kolon isimlerini source'dan alıyoruz
    if column_as_is == 1:
        columns = {col: col for col in get_source_columns(cursor_source, source_table)}

    # Target tablo yoksa oluştur
    cursor_target.execute(f"SHOW TABLES LIKE '{target_table}'")
    if cursor_target.fetchone() is None:
        create_target_table(cursor_target, target_table, columns, cursor_source, source_table, column_as_is, target_id)
        return  # Tabloyu oluşturduktan sonra kolon kontrolüne gerek yok

    # MSSQL'de kolonları kontrol et
    source_columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{source_table}'"
    cursor_source.execute(source_columns_query)
    source_columns = [row[0] for row in cursor_source.fetchall()]

    # MySQL'de tablo var mı kontrol et, yoksa oluştur
    target_columns_query = f"SHOW COLUMNS FROM {target_table}"
    cursor_target.execute(target_columns_query)
    target_columns = [row[0] for row in cursor_target.fetchall()]

    # Eksik kolonlar varsa MySQL üzerinde oluştur
    for source_col, target_col in columns.items():
        if source_col not in source_columns:
            logging.error(f"Source table {source_table} missing column {source_col}")
            raise Exception(f"Source table {source_table} missing column {source_col}")
        if target_col not in target_columns:
            data_type, constraint, seed, increment = get_column_data_type_and_identity(cursor_source, source_table, source_col)
            alter_query = f"ALTER TABLE {target_table} ADD COLUMN {target_col} {data_type}"
            logging.debug(f"Target tablosuna yeni kolon ekleniyor: {alter_query}")
            cursor_target.execute(alter_query)



def get_source_columns(cursor_source, source_table):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{source_table}'"
    cursor_source.execute(query)
    source_columns = [row[0] for row in cursor_source.fetchall()]
    return source_columns


# Sürekli sync işlemi
if __name__ == "__main__":
    try:
        mssql_pool = create_mssql_pool()
        mysql_pool = create_mysql_pool()
        sync('conf.json', mssql_pool, mysql_pool)

        """while True:
            sync('conf.json', mssql_pool, mysql_pool)
            time.sleep(1)
        """
    except Exception as e:
        logging.error(f"Hata: {str(e)}")
