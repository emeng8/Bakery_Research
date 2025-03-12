import requests
import pyodbc
import datetime

YELP_API_KEY = "TBwzdanllKVSXaKXb5Qh7JKSV-Z_8_7wLlBzgdrWO-kpyR630l4gUSTo2-6aOpQe8Mjjq4qYpHA-FuyiqUfYQIksoFtnTpagILs-zffLPSCuOpGrBh01NNXuUgHRZ3Yx"
headers = {
    "Authorization": f"Bearer {YELP_API_KEY}",
    "accept": "application/json"
}

def get_food_and_drinks_insights(business_id, locale=None):
    """
    Calls the Yelp Fusion Insights endpoint:
      /businesses/{business_id}/insights/food_and_drinks
    Returns the parsed JSON containing food/drink items, 
    trending info, and raw ingredients.
    """
    base_url = "https://api.yelp.com/v3/businesses"
    url = f"{base_url}/{business_id}/insights/food_and_drinks"
    
    params = {}
    if locale:
        params["locale"] = locale

    print(f"Calling Yelp Insights for {business_id}...")

    response = requests.get(url, headers=headers, params=params)
    print(f"Status code for {business_id}: {response.status_code}")
    
    # Check success
    if response.status_code != 200:
        print(f"Failed to get insights for {business_id}: {response.text}")
        return None

    data = response.json()
    # The structure might look like:
    # {
    #   "food_and_drinks": [
    #       {
    #           "item_name": "Matcha Latte",
    #           "is_trending": True,
    #           "ingredients": ["Milk", "Matcha Powder", "Sugar"]
    #       },
    #       ...
    #   ]
    # }
    return data

def insert_food_and_drinks_sql(business_id, food_and_drinks_list):
    """
    Inserts the food/drink items into the FoodAndDrinkInsights table.
    """
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=Bakery_Research;"
        r"Trusted_Connection=yes;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    if not food_and_drinks_list:
        print(f"No food & drink items to insert for {business_id}.")
        conn.close()
        return

    for item in food_and_drinks_list:
        # Example JSON structure - adapt to actual:
        item_name = item.get("item_name", "Unknown").replace("'", "''")
        is_trending = item.get("is_trending", False)
        
        # If "ingredients" is a list, convert to a comma-separated string or JSON
        ingredients = item.get("ingredients", [])
        if isinstance(ingredients, list):
            ingredients_str = ", ".join(ingredients).replace("'", "''")
        else:
            # Sometimes the API might return a string directly
            ingredients_str = str(ingredients).replace("'", "''")
        
        # Insert into SQL
        # We'll just set is_trending to 1 if True, else 0
        insert_sql = f"""
            INSERT INTO FoodAndDrinkInsights
                (business_id, item_name, is_trending, raw_ingredients, last_updated)
            VALUES (
                '{business_id}', 
                '{item_name}', 
                {(1 if is_trending else 0)}, 
                '{ingredients_str}',
                GETDATE()
            );
        """
        
        cursor.execute(insert_sql)

    conn.commit()
    conn.close()

def main():
    """
    1. Pull existing business IDs from 'CompetitorBakeries'.
    2. For each business, call Yelp food/drinks insights.
    3. Insert results into 'FoodAndDrinkInsights'.
    """
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=Bakery_Research;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch all existing business_ids in 'CompetitorBakeries'
    cursor.execute("SELECT business_id FROM Competitor_Bakeries")
    rows = cursor.fetchall()

    business_ids = [row[0] for row in rows]
    print(f"Found {len(business_ids)} bakeries in SQL.")

    conn.close()

    # Loop each business_id, fetch insights, insert into DB
    for b_id in business_ids:
        data = get_food_and_drinks_insights(b_id)
        if not data:
            # If the request failed or no data returned, skip
            continue
        
        # Assume there's a key "food_and_drinks" in the response
        food_and_drinks_list = data.get("food_and_drinks", [])
        
        # Insert each item
        insert_food_and_drinks_sql(b_id, food_and_drinks_list)

    print("All done!")

if __name__ == "__main__":
    main()
