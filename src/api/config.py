import os
from dotenv import load_dotenv

load_dotenv()

COINGECKO_BASE_URL = os.getenv('COINGECKO_BASE_URL')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')