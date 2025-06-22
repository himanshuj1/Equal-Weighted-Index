from pyspark.sql import SparkSession
from datetime import datetime
import os
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config import START_DATE, END_DATE
from utils.db_utils import create_tables, create_mysql_connection

def init_spark():
    """Initialize Spark session with required MySQL connector."""
    spark = SparkSession.builder \
        .appName("Top100USMarketCap") \
        .config("spark.jars", os.path.expanduser("~/jars/mysql-connector-java-8.0.33.jar")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_sp500_tickers():
    """Fetch S&P 500 tickers from Wikipedia."""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(url)[0]
    return df_sp500['Symbol'].tolist()


def fetch_marketcap_info(tickers):
    """Fetch metadata for tickers including name, sector, and market cap."""
    def fetch(ticker):
        try:
            info = yf.Ticker(ticker).info
            return (
                ticker,
                info.get("shortName", ""),
                info.get("sector", ""),
                info.get("marketCap", 0)
            )
        except:
            return None

    with ThreadPoolExecutor(max_workers=25) as executor:
        return [r for r in executor.map(fetch, tickers) if r and r[3]]


def download_price_data(tickers):
    """Download last 30 days of stock price data."""
    return yf.download(
        tickers=tickers,
        start=START_DATE,
        end=END_DATE,
        group_by="ticker",
        progress=False,
        threads=True
    )


def insert_stocks_metadata(data):
    """Insert stock metadata into the `stocks` table."""
    conn = create_mysql_connection()
    cursor = conn.cursor()

    cursor.executemany(
        """
        INSERT INTO stocks (ticker, name, sector)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            name = VALUES(name),
            sector = VALUES(sector)
        """,
        [(ticker, name, sector) for ticker, name, sector, _ in data]
    )

    conn.commit()
    cursor.close()
    conn.close()


def calculate_market_data(stocks_info, prices):
    """Calculate daily market cap from prices and shares outstanding."""
    market_data = []
    ticker_to_shares = {}

    for ticker, _, _, market_cap in stocks_info:
        try:
            last_close = prices[ticker]["Close"].dropna().iloc[-1]
            shares_outstanding = market_cap / last_close if last_close else 0
            ticker_to_shares[ticker] = shares_outstanding
        except Exception:
            continue

    for ticker in ticker_to_shares:
        if ticker not in prices:
            continue

        shares_out = ticker_to_shares[ticker]
        for date, price in prices[ticker]["Close"].dropna().items():
            price_val = round(float(price), 2)
            market_cap_val = int(price_val * shares_out)
            market_data.append((
                ticker,
                date.strftime("%Y-%m-%d"),
                price_val,
                market_cap_val
            ))

    return market_data


def insert_market_data(spark, data):
    """Insert market data into the `market_data` table using Spark."""
    conn = create_mysql_connection()
    cursor = conn.cursor()

    df = spark.createDataFrame(data, schema=["ticker", "date", "price", "market_cap"])
    rows = df.collect()

    cursor.executemany(
        """
        INSERT INTO market_data (ticker, date, price, market_cap)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            price = VALUES(price),
            market_cap = VALUES(market_cap)
        """,
        [(r['ticker'], r['date'], r['price'], r['market_cap']) for r in rows]
    )

    conn.commit()
    cursor.close()
    conn.close()


def main():
    """Main orchestration function."""
    create_tables()
    spark = init_spark()

    tickers = get_sp500_tickers()
    print(f"📈 Total S&P 500 tickers fetched: {len(tickers)}")

    stocks_info = fetch_marketcap_info(tickers)
    top_100_stocks = sorted(stocks_info, key=lambda x: x[3], reverse=True)[:100]

    insert_stocks_metadata(top_100_stocks)

    all_tickers = [ticker for ticker, _, _, _ in stocks_info]
    price_data = download_price_data(all_tickers)
    market_data = calculate_market_data(stocks_info, price_data)

    insert_market_data(spark, market_data)

    print("✅ Top 500 US stocks fetched and stored successfully.")


if __name__ == "__main__":
    main()
