import requests
import pandas as pd
import streamlit as st
from models import APIParameters, CoingeckoMarketSchema
from typing import List
from dataclasses import asdict

result = []

def get_data(url: str, parameters: APIParameters) -> List[CoingeckoMarketSchema]:
    """return paginated api response from url"""
    input = parameters
    while True:
        response = requests.get(url, params=asdict(input))
        response.raise_for_status()
        data = response.json()
        result.extend(data)
        input.page += 1
        if not data:
            return result

def process_data(data: List[CoingeckoMarketSchema]) -> pd.DataFrame:
    """combines coingecko response data into single pandas dataframe"""
    data_dicts = []
    for obj in data:
        market_data = CoingeckoMarketSchema(**obj)
        data_dicts.append(asdict(market_data))
    
    dataframe = pd.DataFrame(data_dicts)
    print(dataframe)
    return dataframe

def load_data(data: pd.DataFrame, nrows: int) -> pd.DataFrame:
    return data.head(nrows)


def main():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    get_data(url,APIParameters())
    data = process_data(result)
    st.write(load_data(data,50))

main()