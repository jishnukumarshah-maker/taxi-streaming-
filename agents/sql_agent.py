import os
import re
import anthropic
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("config/.env")

SYSTEM_PROMPT = """You are a strictly read-only SQL assistant querying Snowflake for NYC Taxi operations.
Available tables in schema TAXI_DB.PROD:
- FACT_TRIPS (trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, total_amount, pickup_location_id, dropoff_location_id, payment_type)
- AGG_HOURLY_ZONE (pickup_location_id, hour_start, hour_end, total_trips, total_revenue, avg_distance)
- DIM_ZONE (location_id, borough, zone_name, service_zone)
- DIM_PAYMENT_TYPE (payment_type_id, description)

Rules:
1. Generate valid Snowflake SQL.
2. Return ONLY the raw SQL query. No markdown ticks (```), no conversational text.
3. Only SELECT or WITH queries are permitted.
"""

FORBIDDEN_KEYWORDS = ["drop", "delete", "truncate", "update", "insert", "alter", "create", "grant", "revoke", "replace"]

def is_safe_query(sql: str) -> bool:
    cleaned = sql.strip().lower()
    if not (cleaned.startswith("select") or cleaned.startswith("with")):
        return False
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{kw}\b", cleaned):
            return False
    return True

def query_agent(prompt: str):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    resp = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=300,
        temperature=0.0,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    raw_sql = resp.content[0].text.strip()
    raw_sql = re.sub(r"^```sql\s*|\s*```$", "", raw_sql).strip()

    if not is_safe_query(raw_sql):
        raise PermissionError(f"Security Policy Interception: Forbidden command in SQL: {raw_sql}")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TAXI_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TAXI_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PROD")
    )
    cur = conn.cursor()
    cur.execute(raw_sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"sql": raw_sql, "columns": cols, "results": rows}

if __name__ == "__main__":
    q = "What are the top 3 pickup zones with the most trips?"
    print(f"User Query: {q}")
    try:
        res = query_agent(q)
        print(f"Generated SQL: {res['sql']}")
        print(f"Data: {res['results']}")
    except Exception as e:
        print(f"Agent Execution: {e}")
