import pytest
import pandas as pd
from etl_funcs import transform_json_to_dataframe, drop_dataframe_column
from models import CoingeckoMarketSchema
from typing import List
"""
Unit tests for extract, load, and transform functions
"""

@pytest.fixture
def mock_data():
    data = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "image": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1696501400",
        "current_price": 70187,
        "market_cap": 1381651251183,
        "market_cap_rank": 1,
        "fully_diluted_valuation": 1474623675796,
        "total_volume": 20154184933,
        "high_24h": 70215,
        "low_24h": 68060,
        "price_change_24h": 2126.88,
        "price_change_percentage_24h": 3.12502,
        "market_cap_change_24h": 44287678051,
        "market_cap_change_percentage_24h": 3.31157,
        "circulating_supply": 19675987,
        "total_supply": 21000000,
        "max_supply": 21000000,
        "ath": 73738,
        "ath_change_percentage": -4.77063,
        "ath_date": "2024-03-14T07:10:36.635Z",
        "atl": 67.81,
        "atl_change_percentage": 103455.83335,
        "atl_date": "2013-07-06T00:00:00.000Z",
        "roi": "",
        "last_updated": "2024-04-07T16:49:31.736Z"
    }
    ]
    return data

def test_transform_json_to_dataframe(mock_data: List[CoingeckoMarketSchema]):
    df = transform_json_to_dataframe(mock_data)
    expected_columns = list(CoingeckoMarketSchema.__annotations__.keys())
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(mock_data)
    assert list(df.columns) == expected_columns

def test_drop_dataframe_column(mock_data: List[CoingeckoMarketSchema]):
    df = transform_json_to_dataframe(mock_data)
    new_df = drop_dataframe_column(df,'roi')
    key_length = len(mock_data[0].keys())
    assert isinstance(new_df, pd.DataFrame)
    assert len(new_df.columns) == key_length - 1