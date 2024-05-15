import snowflake.connector
from models import APIParameters
from config import *
from etl_funcs import get_data, transform_json_to_dataframe, drop_dataframe_column


def main():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    data = get_data(url,APIParameters())
    dataframe = transform_json_to_dataframe(data)
    transformed_dataframe = drop_dataframe_column(dataframe,'roi')
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()
    create_table_query = """
    CREATE OR REPLACE TABLE IF NOT EXISTS cex_tokens (
        id VARCHAR(100),
        symbol VARCHAR(100),
        name VARCHAR(100),
        image VARCHAR(100),
        current_price VARCHAR(100),
        market_cap number,
        market_cap_rank number,
        fully_diluted_valuation number,
        total_volume number,
        high_24h number,
        low_24h number,
        price_change_24h float, 
        price_change_percentage_24h float, 
        market_cap_change_24h number,
        market_cap_change_percentage_24h number,
        circulating_supply number,
        total_supply number,
        max_supply number,
        ath float,
        ath_change_percentage float, 
        ath_date varchar(100),
        atl float, 
        atl_change_percentage float, 
        atl_date varchar(100),
        last_updated varchar(100)
    )
    """
    cur.execute(create_table_query)

    # convert dataframe to rows
    rows = [tuple(x) for x in transformed_dataframe.to_numpy()]

    insert_query = """
    INSERT INTO cex_tokens (
    id, symbol, name, image, current_price, market_cap, market_cap_rank, fully_diluted_valuation,
    total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h,
    market_cap_change_24h, market_cap_change_percentage_24h, circulating_supply, total_supply,
    max_supply, ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date,
    last_updated
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query,rows)

    # commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()

main()