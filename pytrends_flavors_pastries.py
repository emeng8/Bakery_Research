import pyodbc
import pandas as pd
from pytrends.request import TrendReq

# 1) Full list of 20 keywords
SEARCH_TERMS = [
    "taro", "black sesame", "matcha", "hojicha", "gochujang",
    "pandan", "ube", "coconut", "miso", "ginger",
    "lemongrass", "sesame", "croissants", "shokupan", "kouign amann",
    "milk tea", "mooncake", "taiyaki", "egg tart", "red bean"
]

def chunk_keywords(keywords, chunk_size=5):
    """
    Splits a list of keywords into sub-lists (chunks) of size 'chunk_size'.
    Prints debug info about chunking.
    """
    print("[DEBUG] Entering chunk_keywords function...")
    print(f"[DEBUG] chunk_size = {chunk_size}, total keywords = {len(keywords)}")
    for i in range(0, len(keywords), chunk_size):
        chunk = keywords[i : i + chunk_size]
        print(f"[DEBUG] Yielding chunk: {chunk}")
        yield chunk

def get_trends_single_chunk(keyword_chunk, timeframe='today 12-m', geo='US-CA'):
    """
    Calls Google Trends for a single chunk (<=5 keywords).
    Returns a DataFrame with interest_over_time() data.
    Prints debug info about the request and response.
    """
    print("[DEBUG] Entering get_trends_single_chunk function...")
    print(f"[DEBUG] Keyword chunk: {keyword_chunk}")
    print(f"[DEBUG] timeframe = {timeframe}, geo = {geo}")

    pytrends = TrendReq(hl='en-US', tz=360)
    print("[DEBUG] Building payload with PyTrends...")
    pytrends.build_payload(keyword_chunk, timeframe=timeframe, geo=geo)
    print("[DEBUG] Payload built. Now fetching interest_over_time...")

    df = pytrends.interest_over_time()
    print("[DEBUG] interest_over_time() returned a DataFrame of shape:", df.shape)

    # Drop 'isPartial' if it exists
    if 'isPartial' in df.columns:
        df.drop(columns=['isPartial'], inplace=True)
        print("[DEBUG] 'isPartial' column dropped.")

    return df

def insert_trends_into_sql(df):
    """
    Inserts the single-chunk DataFrame into SQL. 
    This example assumes your table has columns named '<keyword>_score' 
    plus 'trend_date' and 'last_updated'.
    """
    print("[DEBUG] Entering insert_trends_into_sql function...")
    print("[DEBUG] DataFrame shape:", df.shape)

    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=Bakery_Research;"
        "Trusted_Connection=yes;"
    )
    print("[DEBUG] Connecting to SQL Server with connection string:", conn_str)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("[DEBUG] Connection successful. Beginning insertion row-by-row...")

    # For each date row in df
    for date_idx, row in df.iterrows():
        trend_date = date_idx.strftime('%Y-%m-%d')

        # The columns in this chunk's DataFrame
        columns = list(df.columns)  
        print(f"[DEBUG] Inserting row for date {trend_date} with columns: {columns}")

        # Build the partial SQL for columns
        # e.g. if columns=["taro", "black sesame"], 
        # we'll have col_names="taro_score, black_sesame_score"
        col_names = ", ".join([col.replace(" ", "_") + "_score" for col in columns])

        # Build the values string (the integer scores)
        col_values = []
        for col in columns:
            val = int(row.get(col, 0))
            col_values.append(str(val))
        values_str = ", ".join(col_values)

        # Construct the INSERT statement
        insert_sql = f"""
        INSERT INTO TrendingFlavorsAndPastries (
            trend_date,
            {col_names},
            last_updated
        )
        VALUES (
            '{trend_date}',
            {values_str},
            GETDATE()
        );
        """

        print(f"[DEBUG] Executing SQL: {insert_sql}")
        cursor.execute(insert_sql)

    conn.commit()
    conn.close()
    print("[DEBUG] Data insertion complete. Connection closed.")

def main():
    # 1) Break all 20 keywords into chunks of 5
    print("[DEBUG] Starting main function. Generating chunks...")
    chunks = list(chunk_keywords(SEARCH_TERMS, 5))
    total_chunks = len(chunks)
    print(f"[DEBUG] Total chunks created: {total_chunks}")

    # 2) We will ALWAYS run just chunk index 0 in this script
    chunk_index = 0
    print(f"[DEBUG] We are going to run ONLY chunk index {chunk_index}.")
    chunk = chunks[chunk_index]
    print(f"[DEBUG] Chunk #{chunk_index} = {chunk}")

    # 3) Fetch data for this single chunk
    try:
        df_chunk = get_trends_single_chunk(chunk, timeframe='today 12-m', geo='US-CA')
    except Exception as e:
        print("[ERROR] Exception while fetching trends:", e)
        return

    if df_chunk.empty:
        print("[DEBUG] No data returned or blocked by Google for this chunk. Exiting.")
        return

    # 4) Insert data into SQL
    insert_trends_into_sql(df_chunk)
    print(f"[DEBUG] Done processing chunk index {chunk_index}.")

if __name__ == "__main__":
    print("[DEBUG] Script started. Calling main()...")
    main()
    print("[DEBUG] Script finished.")
