# Equal-Weighted Top 100 US Stocks Index Tracker

This project constructs and tracks an **equal-weighted index** of the **top 100 US stocks by market capitalization**, updating it daily over the past month. The system is designed for modularity, reproducibility, and ease of analysis via SQL-like queries and Excel outputs.

---

## 📌 Objective

Build a data engineering pipeline that:
- Fetches market data for US stocks.
- Selects the top 100 stocks by market cap daily.
- Constructs and tracks an equal-weighted index.
- Logs daily index composition, changes, and performance metrics.
- Outputs final insights to a structured Excel file.

---

##  Project Structure


├── output/
│   └── index_report.xlsx             # Final output Excel with multiple analysis sheets
├── utils/
│   ├── db_utils.py                   # Database connection and table creation
│   └── config.py                     # Configuration file for database connection and dates
├── scripts/
│   ├── fetch_top100_marketcap_1.py   # Fetches stock data and stores it in DuckDB
│   ├── construct_index_2.py          # Constructs the index based on top 100 stocks
│   ├── track_composition_3.py        # Tracks index changes and daily composition
│   └── analysis.py                   # Computes and exports final performance metrics
├── README.md                         # Project documentation
└── driver.py                         # Driver script to run the pipeline






1. **Data Acquisition (`fetch_top100_marketcap.py`)**
   - Source: Yahoo Finance via `yfinance`
   - Fetches historical data (price & market cap) for a wide universe of US stocks.
   - Stores data in a DuckDB in-memory database.

2. **Data Storage**
   - In-memory SQL via DuckDB
   - Tables: `stocks`, `prices`, `market_caps`

3. **Index Construction (`construct_index.py`)**
   - For each day:
     - Select top 100 stocks by market cap.
     - Assign equal weight (1%).
     - Calculate the notional index value.

4. **Composition Tracking (`track_composition.py`)**
   - Tracks:
     - Daily index constituents
     - Additions and removals compared to previous day

5. **Analysis & Export (`analysis.py`)**
   - Calculates:
     - Daily returns, cumulative return, volatility
     - Best/worst days
     - Composition changes count
   - Exports all outputs to a multi-sheet Excel file

##  Excel Output Structure

The generated `index_report.xlsx` includes:

| Sheet Name           | Contents                                                                 |
|----------------------|--------------------------------------------------------------------------|
| `index_performance`  | Date-wise index value, daily % return, cumulative return                 |
| `daily_composition`  | Top 100 tickers for each day                                             |
| `composition_changes`| Date-wise additions and removals from the index                          |
| `summary_metrics`    | Total changes, best/worst days, aggregate stats                          |


## ⚙ Setup Instructions

### Prerequisites

- Python 3.8+
- Install required packages:
  ```bash
  pip install pandas yfinance openpyxl pyspark mysql-connector-python


### Environment Variables

- Update the `.env` file in the root directory with the following variables:
  ```bash
  MYSQL_HOST=localhost
  MYSQL_PORT=3306
  MYSQL_USER=root
  MYSQL_PASSWORD=
  MYSQL_DATABASE=stock_index


### Running the Pipeline

- Run the driver script:
  ```bash
  python driver.py


### Output

- The pipeline will generate an `index_report.xlsx` file in the `output` directory.




#  Equal-Weighted Index Construction using US Stock Market Data

##  Analysis Process, Key Findings, and Design Decisions

---

## 🔬 Analysis Process

### 📥 Data Collection
- Utilized **`yfinance`** to fetch historical data for US stocks.
- Retrieved:
  - Daily price data
  - Market capitalization figures

---

### 🗄️ SQL Database Setup
- Used **MySQL** for fast, in-memory SQL operations.
- Created normalized tables:
  - `stocks`
  - `prices`
  - `market_caps`
  - `index_composition`

---

### 🏗️ Index Construction Logic
- For each trading day in the **past month**:
  - Queried **top 100 stocks by market cap** using SQL.
  - Assigned **equal weight** to each stock.
  - Calculated index value as the **average notional return**.
  - Stored **daily returns** and **index values**.

---

### 📈 Index Tracking
- Tracked changes in index constituents over time using **SQL set operations**.
- Identified:
  - Stocks **added or removed** each day

---

### 📤 Export to Excel
- Final insights were exported to Excel using:
  - `pandas` for data manipulation
  - `openpyxl` for Excel writing
- Export included:
  - Index performance metrics
  - Daily composition
  - Change logs
  - Summary statistics

---

