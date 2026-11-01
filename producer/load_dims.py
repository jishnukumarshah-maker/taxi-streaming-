import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("config/.env")

def seed_zones():
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    print(f"Downloading NYC taxi zone reference from {url}...")
    try:
        df = pd.read_csv(url)
    except Exception as e:
        print(f"Remote fetch failed ({e}), loading local fallback...")
        df = pd.DataFrame([
            {"LocationID": 1, "Borough": "EWR", "Zone": "Newark Airport", "service_zone": "EWR"},
            {"LocationID": 132, "Borough": "Queens", "Zone": "JFK Airport", "service_zone": "Airports"},
            {"LocationID": 230, "Borough": "Manhattan", "Zone": "Times Sq/Theatre District", "service_zone": "Yellow Zone"},
            {"LocationID": 161, "Borough": "Manhattan", "Zone": "Midtown Center", "service_zone": "Yellow Zone"}
        ])

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TAXI_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TAXI_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PROD")
    )
    cur = conn.cursor()
    
    rows = [
        (int(r["LocationID"]), str(r["Borough"]), str(r["Zone"]), str(r["service_zone"])) 
        for _, r in df.iterrows()
    ]
    
    cur.executemany("""
        MERGE INTO DIM_ZONE target
        USING (SELECT %s AS loc_id, %s AS b, %s AS z, %s AS sz) src
        ON target.location_id = src.loc_id
        WHEN MATCHED THEN UPDATE SET borough = src.b, zone_name = src.z, service_zone = src.sz
        WHEN NOT MATCHED THEN INSERT (location_id, borough, zone_name, service_zone)
        VALUES (src.loc_id, src.b, src.z, src.sz);
    """, rows)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully populated {len(rows)} records into DIM_ZONE.")

if __name__ == "__main__":
    seed_zones()
