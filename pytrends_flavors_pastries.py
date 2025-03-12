import pyodbc
import pandas as pd
import sys
from pytrends.request import TrendReq

# 1) Full list of 20 keywords
SEARCH_TERMS = [
    "taro", "black sesame", "matcha", "hojicha", "gochujang",
    "pandan", "ube", "coconut", "miso", "ginger",
    "lemongrass", "sesame", "croissants", "shokupan", "kouign amann",
    "milk tea", "mooncake", "taiyaki", "egg tart", "red bean"
]

def chunk_keywords(keywords, chunk_size=5):
    """ Splits a list of keywords into sub-lists (chunks) of size 'chunk_size'. """
    for i in range(0, len(keywords), chunk_size):
        yield keywords[i : i + chunk_size]

def get_trends_single_chunk(keyword_chunk, timeframe='today 12-m', geo='US-CA'):
    """
    Calls Google Trends for a single chunk (<=5 keywords).
    Returns a DataFrame with interest_over_time() data.
    """
    pytrends = TrendReq(hl='en-US', tz=360)
    pytrends.build_payload(keyword_chunk, timeframe=timeframe, geo=geo)
    df = pytrends.interest_over_time()

    # Drop 'isPartial' if it exists
    if 'isPartial' in df.columns:
        df.drop(columns=['isPartial'], inplace=True)

    return df

def insert_trends_into_sql(df):
    """
    Inserts the single-chunk DataFrame into SQL. 
    Adjust your table columns as needed for all keywords in that chunk.
    """
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=Bakery_Research;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for date_idx, row in df.iterrows():
        trend_date = date_idx.strftime('%Y-%m-%d')
        
        # Collect any columns we might have in 'row':
        # Because each chunk can have up to 5 columns, let's dynamically handle them.
        # Example approach: insert columns by name.
        # If you have 5 specific columns in each chunk, you can do a custom insert. 
        # We'll do a quick example assuming we know them.

        columns = list(df.columns)  # e.g. ["taro", "black sesame", ...]
        # Build the partial SQL for columns
        col_names = ", ".join([col.replace(" ", "_") + "_score" for col in columns])
        # e.g. "taro_score, black_sesame_score"

        # Build placeholders for values (the integer scores)
        col_values = []
        for col in columns:
            val = int(row.get(col, 0))
            col_values.append(str(val))
        values_str = ", ".join(col_values)

        # We'll assume your table has columns named like "taro_score" if col="taro"
        # If your table has 20 static columns, you could do a partial insert or an update approach.
        # For demonstration, let's do a generic approach. 
        # We require that your table can accept NULL or skip columns for those not present in this chunk.

        # If you're specifically storing all 20 columns, you might do a partial update or have 20 columns all the time.
        # But for clarity, let's do a single-chunk approach with a custom table or a dynamic approach.

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
        cursor.execute(insert_sql)

    conn.commit()
    conn.close()
    print("Data inserted for this chunk!")

def main():
    # 1) Break all 20 keywords into chunks of 5
    chunks = list(chunk_keywords(SEARCH_TERMS, 5))
    total_chunks = len(chunks)

    # 2) Determine which chunk index to run (e.g. pass as script arg)
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <CHUNK_INDEX>")
        print(f"Valid chunk indexes: 0 to {total_chunks-1}")
        return
    
    chunk_index = int(sys.argv[1])
    if chunk_index < 0 or chunk_index >= total_chunks:
        print(f"Invalid chunk index. Must be between 0 and {total_chunks-1}.")
        return

    chunk = chunks[chunk_index]
    print(f"Running chunk #{chunk_index} with keywords: {chunk}")

    # 3) Fetch data for this single chunk
    try:
        df_chunk = get_trends_single_chunk(chunk, timeframe='today 12-m', geo='US-CA')
    except Exception as e:
        print("Error fetching trends:", e)
        return

    if df_chunk.empty:
        print("No data returned or blocked by Google for this chunk.")
        return

    # 4) Insert data into SQL
    insert_trends_into_sql(df_chunk)
    print(f"Chunk {chunk_index} complete. Run again for the next chunk if desired.")

if __name__ == "__main__":
    main()
