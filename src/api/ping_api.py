import requests
from api.config import COINGECKO_BASE_URL, COINGECKO_API_KEY

def ping_api():
    url = COINGECKO_BASE_URL + '/ping'
    params = {
        'x_cg_demo_api_key': COINGECKO_API_KEY
    }

    response = requests.get(url, params=params)
    return response.status_code, response.json()