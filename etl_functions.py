import requests
import pandas as pd
import numpy as np
from models import APIParameters, CoingeckoMarketSchema
from typing import List
from dataclasses import asdict

def get_data(url: str, parameters: APIParameters) -> List[CoingeckoMarketSchema]:
    """return paginated api response from url"""
    result = []
    input = parameters
    while True:
        response = requests.get(url, params=asdict(input))
        response.raise_for_status()
        data = response.json()
        result.extend(data)
        input.page += 1
        if not data:
            return result

def transform_json_to_dataframe(data: List[CoingeckoMarketSchema]) -> pd.DataFrame:
    """combines coingecko response data into single pandas dataframe"""
    data_dicts = []
    for obj in data:
        market_data = CoingeckoMarketSchema(**obj)
        data_dicts.append(asdict(market_data))
    dataframe = pd.DataFrame(data_dicts) 
    return dataframe

def drop_dataframe_column(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """drops column from dataframe and replaces all NaN values to None"""
    transformed_dataframe = dataframe.drop(column,axis=1)
    final_dataframe = transformed_dataframe.replace(np.nan, None)
    return final_dataframe

def load_data(data: pd.DataFrame, nrows: int) -> pd.DataFrame:
    """returns summary of data given n rows"""
    return data.head(nrows)