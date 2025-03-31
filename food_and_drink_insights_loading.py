import requests
import pyodbc
import datetime
import ast

YELP_API_KEY = "TBwzdanllKVSXaKXb5Qh7JKSV-Z_8_7wLlBzgdrWO-kpyR630l4gUSTo2-6aOpQe8Mjjq4qYpHA-FuyiqUfYQIksoFtnTpagILs-zffLPSCuOpGrBh01NNXuUgHRZ3Yx"
headers = {
    "Authorization": f"Bearer {YELP_API_KEY}",
    "accept": "application/json"
}

def get_food_and_drinks_insights(business_id):
    url = f"https://api.yelp.com/v3/businesses/{business_id}/insights/food_and_drinks"
    print(f"Calling Yelp Insights for {business_id}...")
    response = requests.get(url, headers=headers)
    print(f"Status code for {business_id}: {response.status_code}")

    if response.status_code != 200:
        print(f"Failed to get insights for {business_id}: {response.text}")
        return None

    return response.json()

def parse_food_dict(raw_data):
    """Parses food entries that may be a dict of '00 =' strings or already a list of dicts."""
    parsed_items = []

    if isinstance(raw_data, dict):
        for key, value in raw_data.items():
            try:
                if isinstance(value, str):
                    value = ast.literal_eval(value.strip())
                parsed_items.append(value)
            except Exception as e:
                print(f"Skipping item {key}: Failed to parse -> {e}")

    elif isinstance(raw_data, list):
        for value in raw_data:
            if isinstance(value, dict):
                parsed_items.append(value)

    return parsed_items


def insert_item_sql(cursor, business_id, item, item_type):
    item_name = item.get("name", "Unknown").replace("'", "''")
    print(f"Processing {item_type}: {item_name}")

    tags_str = ", ".join(item.get("tags", [])).replace("'", "''")
    ingredients = item.get("ingredients", []) if "ingredients" in item else []
    ingredients_str = ", ".join(ingredients).replace("'", "''")
    mentions = item.get("mentions", {})
    mentions_reviews = mentions.get("reviews", 0)
    mentions_photos = mentions.get("photos", 0)

    insert_sql = f"""
        INSERT INTO FoodAndDrinkInsights
            (business_id, item_name, tags, mentions_reviews, mentions_photos, raw_ingredients, item_type, last_updated)
        VALUES (
            '{business_id}',
            '{item_name}',
            '{tags_str}',
            {mentions_reviews},
            {mentions_photos},
            '{ingredients_str}',
            '{item_type}',
            GETDATE()
        );
    """
    cursor.execute(insert_sql)

def insert_food_and_drinks_sql(business_id, data):
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=Bakery_Research;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    food_raw = data.get("food", {})
    drinks_raw = data.get("drinks", [])
    food_items = parse_food_dict(food_raw)
    drink_items = drinks_raw if isinstance(drinks_raw, list) else []

    for food in food_items:
        insert_item_sql(cursor, business_id, food, "food")

    for drink in drink_items:
        insert_item_sql(cursor, business_id, drink, "drink")

    conn.commit()
    conn.close()
    print(f"Inserted food and drink items for {business_id}.")

def main():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=Bakery_Research;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT business_id FROM Competitor_Bakeries")
    rows = cursor.fetchall()
    business_ids = [row[0] for row in rows]
    conn.close()

    print(f"Found {len(business_ids)} business_ids to process.")

    for b_id in business_ids:
        data = get_food_and_drinks_insights(b_id)
        if not data:
            continue
        insert_food_and_drinks_sql(b_id, data)

    print("All done!")

if __name__ == "__main__":
    main()
