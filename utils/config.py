from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()  # Load .env file

MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'stock_index')

START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
END_DATE = datetime.now().strftime('%Y-%m-%d')