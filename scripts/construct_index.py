from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, round, avg, lag, lit
from pyspark.sql.window import Window

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import create_mysql_connection, create_tables
from utils.config import START_DATE, END_DATE



def init_spark():
    """
    Initialize Spark session with MySQL connector jar.
    """
    spark = SparkSession.builder \
        .appName("IndexConstruction") \
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


def calculate_equal_weight_index(df):
    """
    Calculate an equal-weighted index for the top 100 stocks by market cap per day.
    Returns a DataFrame with index values and calculated returns.
    """
    df = df.filter((col("date") >= lit(START_DATE)) & (col("date") <= lit(END_DATE)))

    # Rank stocks by market cap for each day
    rank_window = Window.partitionBy("date").orderBy(col("market_cap").desc())
    ranked_df = df.withColumn("rank", row_number().over(rank_window)).filter(col("rank") <= 100)

    # Calculate equal-weighted index as average price per day
    index_df = ranked_df.groupBy("date").agg(round(avg("price"), 2).alias("index_value"))

    # Window to calculate previous day values
    return_window = Window.orderBy("date")

    # Calculate daily return (%)
    index_df = index_df.withColumn(
        "daily_return",
        round(((col("index_value") / lag("index_value").over(return_window)) - 1) * 100, 2)
    )

    # Calculate daily change in index value
    index_df = index_df.withColumn(
        "daily_change",
        round(col("index_value") - lag("index_value").over(return_window), 2)
    )

    # Calculate cumulative return (%)
    first_index_value = index_df.select("index_value").orderBy("date").first()["index_value"]
    index_df = index_df.withColumn(
        "cumulative_return",
        round(((col("index_value") / lit(first_index_value)) - 1) * 100, 2)
    )

    return index_df


def insert_to_mysql_index_values(df):
    """
    Insert or update index values into MySQL table.
    """
    conn = create_mysql_connection()
    cursor = conn.cursor()

    records = [
        (str(row['date']), row['index_value'], row['daily_return'], row['daily_change'], row['cumulative_return'])
        for row in df.collect()
    ]

    cursor.executemany(
        """
        INSERT INTO index_values (date, index_value, daily_return, daily_change, cumulative_return)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            index_value=VALUES(index_value),
            daily_return=VALUES(daily_return),
            daily_change=VALUES(daily_change),
            cumulative_return=VALUES(cumulative_return)
        """,
        records
    )
    conn.commit()
    cursor.close()
    conn.close()


def main():
    create_tables()
    spark = init_spark()
    market_df = fetch_market_data(spark)
    index_df = calculate_equal_weight_index(market_df)
    insert_to_mysql_index_values(index_df)
    print("✅ Index construction done and stored.")


if __name__ == "__main__":
    main()
