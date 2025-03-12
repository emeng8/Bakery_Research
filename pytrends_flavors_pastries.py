import pyodbc
import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime

# ----- 1) Define your flavors & pastries including new ones -----
SEARCH_TERMS = [
    "taro", "black sesame", "matcha", "hojicha", "gochujang",
    "pandan", "ube", "coconut", "miso", "ginger",
    "lemongrass", "sesame", "croissants", "shokupan", "kouign amann",
    "milk tea", "mooncake", "taiyaki", "egg tart", "red bean"
]

def chunk_keywords(keywords, chunk_size=5):
    for i in range(0, len(keywords), chunk_size):
        yield keywords[i : i + chunk_size]

chunks = list(chunk_keywords(SEARCH_TERMS, 5))
print("Keyword chunks:", chunks)


def get_trends_chunked(chunks, timeframe='today 12-m', geo='US-CA-803'):
    pytrends = TrendReq(hl='en-US', tz=360)
    final_df = pd.DataFrame()

    for chunk in chunks:
        print(f"Building payload for chunk: {chunk}")
        pytrends.build_payload(kw_list=chunk, timeframe=timeframe, geo=geo)
        df_chunk = pytrends.interest_over_time()

        # Remove isPartial if present
        if 'isPartial' in df_chunk.columns:
            df_chunk.drop(columns=['isPartial'], inplace=True)

        # If final_df is empty, just take df_chunk
        if final_df.empty:
            final_df = df_chunk
        else:
            # Merge on the date index
            final_df = final_df.join(df_chunk, how='outer')
    
    return final_df

chunks = list(chunk_keywords(SEARCH_TERMS, 5))
df_trends = get_trends_chunked(chunks, timeframe='today 12-m', geo='US-CA-803')
print(df_trends.head())


def insert_trends_into_sql(df):
    """
    Inserts Google Trends data into the TrendingFlavorsAndPastries table.
    df columns will be each search term. 
    The index is a datetime (each row is a specific date/week).
    """
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"  # or your server name/instance
        "Database=Bakery_Research;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Iterate each row in df and build INSERT statement
    for date_idx, row in df.iterrows():
        trend_date = date_idx.strftime('%Y-%m-%d')
        
        # Safely retrieve each search term's score, defaulting to 0
        taro_score            = int(row.get("taro", 0))
        black_sesame_score    = int(row.get("black sesame", 0))
        matcha_score          = int(row.get("matcha", 0))
        hojicha_score         = int(row.get("hojicha", 0))
        gochujang_score       = int(row.get("gochujang", 0))
        pandan_score          = int(row.get("pandan", 0))
        ube_score             = int(row.get("ube", 0))
        coconut_score         = int(row.get("coconut", 0))
        miso_score            = int(row.get("miso", 0))
        ginger_score          = int(row.get("ginger", 0))
        lemongrass_score      = int(row.get("lemongrass", 0))
        sesame_score          = int(row.get("sesame", 0))
        croissants_score      = int(row.get("croissants", 0))
        shokupan_score        = int(row.get("shokupan", 0))
        kouign_amann_score    = int(row.get("kouign amann", 0))
        milk_tea_score        = int(row.get("milk tea", 0))
        mooncake_score        = int(row.get("mooncake", 0))
        taiyaki_score         = int(row.get("taiyaki", 0))
        egg_tart_score        = int(row.get("egg tart", 0))
        red_bean_score        = int(row.get("red bean", 0))

        insert_sql = f"""
        INSERT INTO dbo.TrendingFlavorsAndPastries (
            trend_date,
            taro_score,
            black_sesame_score,
            matcha_score,
            hojicha_score,
            gochujang_score,
            pandan_score,
            ube_score,
            coconut_score,
            miso_score,
            ginger_score,
            lemongrass_score,
            sesame_score,
            croissants_score,
            shokupan_score,
            kouign_amann_score,
            milk_tea_score,
            mooncake_score,
            taiyaki_score,
            egg_tart_score,
            red_bean_score,
            last_updated
        )
        VALUES (
            '{trend_date}',
            {taro_score},
            {black_sesame_score},
            {matcha_score},
            {hojicha_score},
            {gochujang_score},
            {pandan_score},
            {ube_score},
            {coconut_score},
            {miso_score},
            {ginger_score},
            {lemongrass_score},
            {sesame_score},
            {croissants_score},
            {shokupan_score},
            {kouign_amann_score},
            {milk_tea_score},
            {mooncake_score},
            {taiyaki_score},
            {egg_tart_score},
            {red_bean_score},
            GETDATE()
        );
        """
        cursor.execute(insert_sql)

    conn.commit()
    conn.close()
    print("Data successfully inserted into SQL!")


def main():
    # 1) Create chunks of keywords (5 each)
    chunks = list(chunk_keywords(SEARCH_TERMS, 5))

    # 2) Attempt to fetch LA region data with chunking
    # If 'US-CA-803' fails, fallback to 'US-CA'
    try:
        df_trends = get_trends_chunked(chunks, timeframe='today 12-m', geo='US-CA-803')
        if df_trends.empty:
            print("No data for 'US-CA-803'. Retrying with 'US-CA'.")
            df_trends = get_trends_chunked(chunks, timeframe='today 12-m', geo='US-CA')
    except Exception as e:
        print("Error with 'US-CA-803':", e)
        print("Retrying with 'US-CA'.")
        df_trends = get_trends_chunked(chunks, timeframe='today 12-m', geo='US-CA')

    print("Final DataFrame shape:", df_trends.shape)
    if df_trends.empty:
        print("No data returned from PyTrends. Check your keywords or parameters.")
        return

    # 3) Insert to SQL
    insert_trends_into_sql(df_trends)
    print("Done!")

if __name__ == "__main__":
    main()