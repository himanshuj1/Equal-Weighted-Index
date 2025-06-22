from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, lit
from datetime import datetime
import mysql.connector
import os
from time import sleep

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import create_mysql_connection, create_tables
from utils.config import START_DATE, END_DATE


def init_spark():
    """
    Initialize Spark session with MySQL connector jar.
    """
    spark = SparkSession.builder \
        .appName("TrackComposition") \
        .config("spark.jars", os.path.expanduser("~/jars/mysql-connector-java-8.0.33.jar")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def fetch_market_data(spark):
    """
    Fetch market data from MySQL and return as Spark DataFrame.
    """
    conn = create_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT ticker, date, price, market_cap FROM market_data")
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return spark.createDataFrame(results)


def write_daily_composition(df):
    """
    Insert or update daily composition records into MySQL.
    """
    conn = create_mysql_connection()
    cursor = conn.cursor()
    records = [(str(row['date']), row['ticker'], row['weight']) for row in df.collect()]
    cursor.executemany(
        """
        INSERT INTO daily_composition (date, ticker, weight)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            weight = VALUES(weight)
        """,
        records
    )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Daily composition inserted into MySQL.")


def fetch_daily_composition_dates(spark):
    """
    Fetch distinct dates from daily_composition table.
    """
    conn = create_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT date, ticker FROM daily_composition")
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return spark.createDataFrame(results)


def track_composition_changes(spark):
    """
    Compute changes in composition day-over-day and insert into MySQL.
    """
    daily_df = fetch_daily_composition_dates(spark)
    dates = [row.date for row in daily_df.select("date").distinct().sort("date").collect()]

    changes = []
    for i in range(1, len(dates)):
        prev_date, curr_date = dates[i - 1], dates[i]

        prev_tickers = set(daily_df.filter(col("date") == prev_date).select("ticker").rdd.flatMap(lambda x: x).collect())
        curr_tickers = set(daily_df.filter(col("date") == curr_date).select("ticker").rdd.flatMap(lambda x: x).collect())

        added = ",".join(sorted(curr_tickers - prev_tickers))
        removed = ",".join(sorted(prev_tickers - curr_tickers))

        changes.append((curr_date, added, removed))

    # Insert changes into DB
    conn = create_mysql_connection()
    cursor = conn.cursor()
    for date, added, removed in changes:
        cursor.execute(
            """
            INSERT INTO composition_changes (date, added, removed)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE added=VALUES(added), removed=VALUES(removed)
            """,
            (date.strftime("%Y-%m-%d"), added, removed)
        )
    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Composition changes tracked and inserted.")


def compute_daily_composition(df):
    """
    Rank stocks by market cap per day and assign equal weights to top 100.
    """
    window_spec = Window.partitionBy("date").orderBy(col("market_cap").desc())
    ranked_df = df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 100)
    return ranked_df.select("date", "ticker").withColumn("weight", lit(0.01))


def main():
    create_tables()
    spark = init_spark()

    market_df = fetch_market_data(spark)
    composition_df = compute_daily_composition(market_df)
    write_daily_composition(composition_df)

    # Pause to ensure DB updates are reflected (if needed)
    sleep(10)

    track_composition_changes(spark)
    print("✅ Daily composition and changes stored.")


if __name__ == "__main__":
    main()
