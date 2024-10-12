[![](./logo.png)](https://www.waitfordata.com)

# MSSQL to MySQL Sync Application
This application allows you to seamlessly transfer datasets from a MSSQL database to a MySQL database and keep them synchronized. The application is designed to be flexible, allowing users to customize table and column names, apply filters to the data during transfer, and configure synchronization frequency via a JSON configuration file.

## Features
**1. MSSQL to MySQL Data Transfer**
   -   The application retrieves datasets from a specified MSSQL database and transfers them to a target MySQL database.
   -   This is achieved using a connection pool for efficient database interaction and minimal overhead.

**2. Customizable Table and Column Mapping via JSON**
   -   Users can define how table names and column names are mapped between the source MSSQL database and the target MySQL database.
   -   This can be done using the conf.json file where:
         -   You can rename tables and columns.
         -   You can also transfer tables and columns as-is without any name changes.
   -   Additionally, you can apply a WHERE clause to filter the data being transferred, which is also specified in the conf.json file.

**3. Continuous Synchronization**
   -   Once the initial transfer is complete, the application continuously synchronizes the data between the MSSQL and MySQL databases.
   -   The synchronization process ensures that any changes in the MSSQL database are reflected in the MySQL database.
   -   The frequency of this sync operation is configurable via the conf.json file, using the frequency value (in seconds).

**4. Data Type Mapping Between MSSQL and MySQL**
   -   During the table creation in MySQL, the following MSSQL data types are automatically mapped to their MySQL equivalents:


| MSSQL Data Type    | MySQL Equivalent      |
|--------------------|-----------------------|
| `bigint`           | `BIGINT`              |
| `smallint`         | `SMALLINT`            |
| `tinyint`          | `TINYINT`             |
| `float`            | `FLOAT`               |
| `decimal`, `numeric`| `DECIMAL(precision, scale)` |
| `nvarchar`, `varchar`| `VARCHAR(length)` (or `TEXT` for MAX) |
| `char`             | `CHAR(length)`        |
| `binary`           | `LONGBLOB`            |
| `varbinary`        | `LONGBLOB`            |
| `bit`              | `TINYINT(1)`          |
| `date`             | `DATE`                |
| `datetime2`        | `DATETIME(6)`         |
| `datetime`         | `DATETIME`            |
| `smalldatetime`    | `DATETIME`            |
| `datetimeoffset`   | `VARCHAR(40)`         |
| `image`            | `LONGBLOB`            |
| `money`            | `DECIMAL(19,4)`       |
| `real`             | `FLOAT`               |
| `smallmoney`       | `DECIMAL(10,4)`       |
| `time`             | `TIME(6)`             |
| `text`             | `LONGTEXT`            |
| `ntext`            | `LONGTEXT`            |
| `xml`              | `TEXT`                |
| `uniqueidentifier` | `CHAR(36)`            |


   -   Auto Increment Columns: If a column in MSSQL is an identity column (AUTO_INCREMENT), it will be mapped to AUTO_INCREMENT in MySQL and treated as the primary key.

**5. Ignored Data Types**

The following data types are ignored during the transfer and not included in the synchronization process:
   -   TIME(6)
   -   BINARY
   -   VARBINARY
   -   LONGBLOB

These data types are excluded to prevent any inconsistencies or mismatches during data comparison and synchronization.

## Configuration (conf.json)
All customization is done via a configuration file named conf.json. Below is an example of how this file can be structured:

```json
{
  "frequency": 1,
  "tables": [
    {
      "source_table": "posv3",
      "columns": {
        "ID": "idt",
        "testcolumn15": "tb1",
        "testcolumn16": "tb2",
        "testcolumn17": "tb3"
      },
      "conditions": "ID > 3 AND testcolumn1 = '5'",
      "id_column": "ID",
      "table_as_is": 1,
      "column_as_is": 0,
      "target_id": "idt"
    },

    {
      "source_table": "posv4",
      "id_column": "ID",
      "table_as_is": 1,
      "column_as_is": 1
    }
  ]
}
```


## Key Options in conf.json:
   -   **source_table**: The table in the MSSQL database to transfer.
   -   **target_table**: The table name in MySQL (can be omitted if table_as_is = 1).
   -   **columns**: A mapping of columns between MSSQL and MySQL. Leave this out if column_as_is = 1.
   -   **conditions**: Optional WHERE clause for filtering rows during transfer.
   -   **id_column**: The primary key column for identifying rows. (SOURCE)
   -   **target_id**: The primary key column. (TARGET)
   -   **table_as_is**: Set to 1 to transfer the table without renaming it.
   -   **column_as_is**: Set to 1 to transfer the columns without renaming them.
   -   **frequency**: The frequency (in seconds) for the continuous synchronization process.


## Running the Application

1- Configure the conf.json file according to your requirements.

2- Make sure the .env file has the appropriate connection settings for both MSSQL and MySQL.

3- Run the application:

```
python main.py
```
The application will perform the initial transfer and continue syncing data based on the defined frequency.

## Requirements
   -   **Python 3.x**
   -   **Libraries:** pyodbc, mysql-connector-python, dotenv, json, time, logging

Install dependencies via pip:
```
pip install -r requirements.txt
```








