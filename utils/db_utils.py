import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import mysql.connector
from mysql.connector import Error
from utils.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
from typing import Optional


def create_mysql_connection() -> Optional[mysql.connector.connection.MySQLConnection]:
    """
    Creates and returns a MySQL database connection using credentials from config.
    Returns None if connection fails.
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        return conn
    except Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None


def create_tables() -> None:
    """
    Creates required tables in the database if they don't exist.
    Uses context managers to safely handle connections and cursors.
    """
    conn = create_mysql_connection()
    if not conn:
        raise ConnectionError("Failed to connect to the database. Tables not created.")

    create_stocks_table = """
    CREATE TABLE IF NOT EXISTS stocks (
        ticker VARCHAR(10) PRIMARY KEY,
        name VARCHAR(255),
        sector VARCHAR(100)
    );
    """

    create_market_data_table = """
    CREATE TABLE IF NOT EXISTS market_data (
        ticker VARCHAR(10),
        date DATE,
        price DOUBLE,
        market_cap DOUBLE,
        PRIMARY KEY (ticker, date)
    );
    """

    create_index_values_table = """
    CREATE TABLE IF NOT EXISTS index_values (
        `date` DATE PRIMARY KEY,
        index_value DOUBLE,
        daily_return DOUBLE,
        daily_change DOUBLE,
        cumulative_return DOUBLE
    );
    """

    create_daily_composition_table = """
    CREATE TABLE IF NOT EXISTS daily_composition (
        date DATE,
        ticker VARCHAR(10),
        weight DOUBLE,
        PRIMARY KEY (date, ticker)
    );
    """

    create_composition_changes_table = """
    CREATE TABLE IF NOT EXISTS composition_changes (
        date DATE PRIMARY KEY,
        added TEXT,
        removed TEXT
    );
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_stocks_table)
            cursor.execute(create_market_data_table)
            cursor.execute(create_index_values_table)
            cursor.execute(create_daily_composition_table)
            cursor.execute(create_composition_changes_table)
        conn.commit()
    except Error as err:
        print(f"Error creating tables: {err}")
        raise
    finally:
        conn.close()
