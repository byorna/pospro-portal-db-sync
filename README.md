[![](./logo.png)](https://www.waitfordata.com)

# Database Synchronization Service
Database Synchronization Service is a Python-based tool designed to synchronize data between an MSSQL source database and a MySQL target database. This service allows for flexible configuration through a JSON file, supporting custom column mappings, identity column handling, and continuous synchronization.

## Features
-  Data synchronization between MSSQL and MySQL: Supports syncing tables from an MSSQL source to a MySQL target.
-  Customizable column mappings: Allows you to specify source and target columns for mapping.
-  Identity column customization: Automatically handles identity columns, with the ability to rename them in the target database.
-  Flexible configurations: Configure synchronization rules through a JSON file, including table and column name mapping, and whether to   keep the table and column names as-is.
-  Connection pooling: Implements connection pools for both MSSQL and MySQL to optimize performance.
-  Logging: Provides detailed logging of synchronization activities, errors, and queries.


## Requirements
-  Python 3.8+
-  MSSQL ODBC Driver (libmsodbcsql)
-  MySQL Connector
-  dotenv

## Installation

To install and set up the **flo-portal-db-sync**, follow the steps below:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/byorna/flo-portal-db-sync.git
   ```

2. **Navigate into the project directory:**
    ```bash
    cd flo-portal-db-sync
    ```

3. **Create the environment file:**
   
   3.1. Copy the `.env.example` file and create a `.env` file:
      ```bash
      cp .env.example .env
      ```
   
   3.2. Edit the .env file with your actual database credentials:
   
      ```bash
      MSSQL_SERVER=
      MSSQL_DB=
      MSSQL_USER=
      MSSQL_PASSWORD=
      
      MYSQL_HOST=
      MYSQL_DB=
      MYSQL_USER=
      MYSQL_PASSWORD=
      ```
   Important Note !
   -   If both db-sync and the MySQL is running in Docker, MYSQL_HOST parameter in the environment file should be set to the Docker hostname.
   -   If db-sync is running in Docker and the MySQL database is running on the host machine, MYSQL_HOST parameter in the environment file should be set to host machine's internal IP address to ensure proper connectivity.


## Running the Application Locally:

4. **Install required Python libraries:**
    ```bash
    pip install -r requirements.txt
    ```

5. **Run the Application:**
   
   ```bash
   python3 main.py
   ```

## Running the Application with Docker:

4. **Build the Docker image:**

      ```bash
      docker build -t portal-db-sync .
      ```

5. **Run the Application Using Docker:**
   
      -   If MySQL database is running on the host machine:

      ```bash
      docker run -d --name portal-db-sync --hostname portal-db-sync --env-file .env --restart always portal-db-sync
      ```

      -   If MySQL database is running in a docker:

      ```bash
      docker run -d --name portal-db-sync --hostname portal-db-sync --env-file .env --network backend --restart always portal-db-sync
      ```
   

# Data Type Mappings Between MSSQL and MySQL
During the synchronization process, the application automatically converts the MSSQL column data types to their closest MySQL equivalents. Below is a detailed explanation of how each MSSQL data type is mapped to a MySQL data type.

## Data Type Mapping Table:

| MSSQL Data Type       | MySQL Equivalent Data Type | Notes |
|-----------------------|---------------------------|-------|
| `bigint`              | `BIGINT`                  | A 64-bit integer. |
| `smallint`            | `SMALLINT`                | A 16-bit integer. |
| `tinyint`             | `TINYINT`                 | A very small integer. |
| `float`               | `FLOAT`                   | Single-precision floating-point number. |
| `decimal`, `numeric`  | `DECIMAL(x,y)`            | Same precision and scale from MSSQL. |
| `nvarchar`, `varchar` | `VARCHAR(x)` or `TEXT`    | Converts `MAX` in MSSQL to `TEXT` in MySQL, otherwise `VARCHAR(x)`. |
| `char`                | `CHAR(x)`                 | Fixed-length string, same length in MySQL. |
| `binary`              | `BINARY(x)`               | Fixed-length binary data, same length in MySQL. |
| `varbinary`           | `VARBINARY(x)` or `LONGBLOB`| If `MAX` in MSSQL, converts to `LONGBLOB` in MySQL. |
| `bit`                 | `TINYINT(1)`              | MySQL uses `TINYINT(1)` for boolean or bit. |
| `date`                | `DATE`                    | Converts directly to MySQL `DATE`. |
| `datetime2`           | `DATETIME(6)`             | MySQL uses 6 digits precision for `DATETIME`. |
| `datetime`            | `DATETIME`                | General date and time value. |
| `smalldatetime`       | `DATETIME`                | MySQL does not have `smalldatetime`, so `DATETIME` is used. |
| `datetimeoffset`       | `VARCHAR(40)`             | MSSQL's `datetimeoffset` is stored as `VARCHAR(40)` in MySQL. |
| `image`               | `LONGBLOB`                | Binary large object, mapped to MySQL's `LONGBLOB`. |
| `money`               | `DECIMAL(19,4)`           | MySQL's equivalent for MSSQL's `money`. |
| `real`                | `FLOAT`                   | MySQL uses `FLOAT` for `real` type. |
| `smallmoney`          | `DECIMAL(10,4)`           | MySQL uses `DECIMAL(10,4)` for `smallmoney`. |
| `time`                | `TIME(6)`                 | MySQL uses 6 digits precision for `TIME`. |
| `text`                | `LONGTEXT`                | Large text data stored as `LONGTEXT` in MySQL. |
| `ntext`               | `LONGTEXT`                | Same as `text`, stored as `LONGTEXT` in MySQL. |
| `xml`                 | `TEXT`                    | XML data is stored as `TEXT` in MySQL. |
| `uniqueidentifier`    | `CHAR(36)`                | MySQL stores `uniqueidentifier` as `CHAR(36)`. |

## Special Handling for Identity Columns:

*  Identity Columns in MSSQL: If a column is marked as an identity column in MSSQL, the script will automatically handle it as an auto-increment column in MySQL.
    -  MSSQL Identity Column: This is determined using the COLUMNPROPERTY function to check if a column is an identity column.
    -  MySQL Equivalent: In MySQL, identity columns are mapped to AUTO_INCREMENT and will be set as the primary key.


# MSSQL table structure
```bash
CREATE TABLE posv3 (
    ID bigint IDENTITY(1,1),   -- Identity column
    testcolumn15 nvarchar(50),
    testcolumn16 smallint,
    testcolumn17 float
);
```

# Converted MySQL structure
```bash
CREATE TABLE posv3 (
    idt BIGINT AUTO_INCREMENT PRIMARY KEY,  -- Identity column becomes AUTO_INCREMENT
    tb1 VARCHAR(50),                        -- nvarchar becomes VARCHAR
    tb2 SMALLINT,                           -- smallint stays as SMALLINT
    tb3 FLOAT                               -- float remains as FLOAT
);
```


# config.json Structure and Scenarios
The config.json file defines the tables and columns for synchronization between the MSSQL source and MySQL target. Below are the available tags and example scenarios for their use.

## source_table:

**Description**: Specifies the name of the table in the MSSQL source database.

**Usage**: 
```bash
"source_table": "posv3"
```

**Scenario**: You have a table named posv3 in your MSSQL database that you want to synchronize with your MySQL database.

## columns:

**Description**: A dictionary that maps columns from the source table to the target table.

**Usage**:

```bash
"columns": {
  "ID": "idt",
  "testcolumn15": "tb1",
  "testcolumn16": "tb2",
  "testcolumn17": "tb3"
}
```

**Scenario**: You want to map the ID column in the source table to the idt column in the target table, and similarly map testcolumn15, testcolumn16, and testcolumn17 to tb1, tb2, and tb3, respectively, in the target table.


## id_column:

**Description**: Specifies the identity column of the source table. This column will be treated as the primary key and will auto-increment in the target table if applicable.

**Usage**: 
```bash
"id_column": "ID"
```

**Scenario**: If your source table uses the ID column as its primary identity column, this will be used to manage record uniqueness and auto-increment in the target table.


## table_as_is:

**Description**: Defines whether the source table name will be preserved in the target database or if a custom target table name should be used.

**Options**:
  -  1: Use the same name for the target table as the source table.
  -  0: Use a custom name for the target table (specified separately with target_table).

**Usage**: 
```bash
"table_as_is": 1
```

**Scenario 1**: If table_as_is is set to 1, the source table (posv3) will also be created as posv3 in the MySQL target database.

**Scenario 2**: If table_as_is is set to 0, you must specify a custom target table name under the target_table tag.


## column_as_is:

**Description**: Defines whether the source column names will be preserved in the target database or if custom column names should be used.

**Options**:
  -  1: Keep the source column names as-is in the target database.
  -  0: Use custom column names specified in the columns tag.

**Usage**: 
```bash
"column_as_is": 0
```

**Scenario 1**: If column_as_is is set to 1, the columns in the source table will have the same names in the target table.

**Scenario 2**: If column_as_is is set to 0, the column mappings specified in the columns tag will be used (e.g., ID in the source table will be renamed to idt in the target table).

## target_id:

**Description**: Specifies the name of the identity column in the target database. This is used if you want to rename the identity column from the source to a custom name in the target.

**Usage**: 
```bash
"target_id": "idt"
```

**Scenario**: You want the identity column in the source table (ID) to be renamed to idt in the target table. This is helpful when you need to have a different naming convention in the target database.



# Example Scenarios:
## Basic Synchronization Keeping Table and Column Names the Same:
```bash
{
  "tables": [
    {
      "source_table": "posv3",
      "id_column": "ID",
      "table_as_is": 1,
      "column_as_is": 1
    }
  ]
}
```

**Explanation**: This will sync the posv3 table from MSSQL to MySQL, keeping both the table name and column names exactly the same.


## Renaming Columns in the Target Table:
```bash
{
  "tables": [
    {
      "source_table": "posv3",
      "columns": {
        "ID": "idt",
        "testcolumn15": "tb1",
        "testcolumn16": "tb2",
        "testcolumn17": "tb3"
      },
      "id_column": "ID",
      "table_as_is": 1,
      "column_as_is": 0
    }
  ]
}
```

**Explanation**: The source table (posv3) will remain as posv3 in MySQL, but the columns will be renamed according to the mappings specified in columns. The identity column ID will be renamed to idt in the target database.

## Using a Custom Table Name in the Target Database:
```bash
{
  "tables": [
    {
      "source_table": "posv3",
      "target_table": "new_posv3",
      "id_column": "ID",
      "table_as_is": 0,
      "column_as_is": 1
    }
  ]
}
```
**Explanation**: The source table posv3 will be renamed to new_posv3 in MySQL, but the column names will remain the same as in the source table.


## Renaming the Identity Column in the Target Database:
```bash
{
  "tables": [
    {
      "source_table": "posv3",
      "columns": {
        "ID": "idt",
        "testcolumn15": "tb1",
        "testcolumn16": "tb2",
        "testcolumn17": "tb3"
      },
      "id_column": "ID",
      "target_id": "idt",
      "table_as_is": 1,
      "column_as_is": 0
    }
  ]
}
```

**Explanation**: The source table posv3 will keep its name in MySQL, but the identity column ID will be renamed to idt, and other columns will be renamed based on the mappings.


**These scenarios illustrate how you can configure the config.json file to handle different use cases, from keeping the table and column names as they are, to renaming columns and tables in the target database.**







## License
This project is licensed under the MIT License - see the LICENSE file for details.








