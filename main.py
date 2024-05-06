import requests
import pandas as pd
from models import APIParameters, CoingeckoMarketSchema
from typing import List
from dataclasses import asdict

def get_data(url: str, parameters: APIParameters) -> List[CoingeckoMarketSchema]:
    """return paginated api response from url"""
    input = parameters
    result = []
    while True:
        response = requests.get(url, params=asdict(input))
        response.raise_for_status()
        data = response.json()
        result.extend(data)
        input.page += 1
        if not data:
            return result

def process_data(data: List[CoingeckoMarketSchema]) -> pd.DataFrame:
    data_dicts = []
    for obj in data:
        market_data = CoingeckoMarketSchema(**obj)
        data_dicts.append(asdict(market_data))
    
    dataframe = pd.DataFrame(data_dicts)
    print(dataframe)
    return dataframe


def main():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    data = get_data(url,APIParameters())
    process_data(data)

main()