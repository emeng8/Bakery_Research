
# find driver
import pyodbc
# print(pyodbc.drivers())

import requests
import datetime 

YELP_API_KEY =  "TBwzdanllKVSXaKXb5Qh7JKSV-Z_8_7wLlBzgdrWO-kpyR630l4gUSTo2-6aOpQe8Mjjq4qYpHA-FuyiqUfYQIksoFtnTpagILs-zffLPSCuOpGrBh01NNXuUgHRZ3Yx"
headers = {
    "Authorization": f"Bearer {YELP_API_KEY}"}



def get_competitor_bakeries(location="Los Angeles", term="bakery", limit=50):

    """
    Calls the Yelp API to fetch bakeries in a specific location.
    Prints details about the request and the response status.
    """
    print(f"Preparing to call Yelp API for '{term}' in '{location}'...")
    print(f"Requesting {limit} results...")
    url = "https://api.yelp.com/v3/businesses/search"
    params = {
        "location": location,
        "term": term,
        "limit": limit
    }
    
    print(f"Making GET request to {url} with params: {params}")

    response = requests.get(url, headers=headers, params=params)

    print("Response status code:", response.status_code)
    if response.status_code == 200:
        print("API call successful!")
    else:
        print("API call failed. Check your API key or parameters.")
        # You might choose to return [] or raise an exception.
        return []
    
    data = response.json()
    businesses = data.get("businesses", [])
    print(f"Number of businesses fetched: {len(businesses)}")
    return businesses
    

def insert_into_sql(businesses):
    print(f"Connecting to SQL Server...")
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=localhost;'
                          'Database=Bakery_Research;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    print("Connection successful. Beginning insertion...")

    for b in businesses:
        business_id = b["id"]
        name = b["name"].replace("'", "''")  # escape single quotes
        rating = b.get("rating", 0.0)
        review_count = b.get("review_count", 0)
        
        location_dict = b.get("location", {})
        address = " ".join(location_dict.get("display_address", [])).replace("'", "''")
        city = location_dict.get("city", "").replace("'", "''")
        zip_code = location_dict.get("zip_code", "")
        
        coordinates = b.get("coordinates", {})
        latitude = coordinates.get("latitude", 0.0)
        longitude = coordinates.get("longitude", 0.0)
        
        price_level = b.get("price", None)  # e.g., "$$"

        print(f"Processing business: {name} (ID: {business_id})")

        # Build INSERT
        insert_query = f"""
            IF NOT EXISTS (SELECT 1 FROM Competitor_Bakeries WHERE business_id = '{business_id}')
            BEGIN
                INSERT INTO Competitor_Bakeries 
                (business_id, name, rating, review_count, address, city, zip_code, latitude, longitude, price_level)
                VALUES (
                    '{business_id}', '{name}', {rating}, {review_count}, 
                    '{address}', '{city}', '{zip_code}', {latitude}, {longitude}, '{price_level}'
                );
            END
        """
        
        cursor.execute(insert_query)

    conn.commit()
    print("All insertion queries executed. Closing connection.")

    conn.close()
    print("Connection closed.")

if __name__ == "__main__":
    # Fetch data
    bakeries_data = get_competitor_bakeries(location="Los Angeles", term="bakery", limit=50)
    # Insert data into SQL
    insert_into_sql(bakeries_data)