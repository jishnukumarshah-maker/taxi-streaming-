import os
import json
import time
import random
import argparse
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv("config/.env")

def delivery_cb(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed on partition {msg.partition()}: {err}")

def create_confluent_producer():
    conf = {
        'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('CONFLUENT_API_KEY'),
        'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
        'acks': 'all',
        'linger.ms': 20,
        'batch.num.messages': 1000,
        'compression.type': 'snappy'
    }
    return Producer(conf)

def run_replay(file_path: str = None, max_events: int = 5000, rate: int = 300):
    producer = create_confluent_producer()
    topic = os.getenv("CONFLUENT_TOPIC", "trips.raw")

    print(f"Preparing dataset for replay (Target: {max_events} events at {rate} msg/sec)...")
    df = None
    if file_path and os.path.exists(file_path):
        df = pd.read_parquet(file_path).head(max_events)
    
    interval = 1.0 / rate
    bad_trip_ids = []
    start_time = time.time()

    for idx in range(max_events):
        trip_id = f"trip_{idx}_{int(time.time() * 1000)}"
        now_iso = datetime.now(timezone.utc).isoformat()

        if df is not None and idx < len(df):
            row = df.iloc[idx]
            v_id = int(row.get("VendorID", 1))
            p_count = int(row.get("passenger_count", 1) if pd.notnull(row.get("passenger_count")) else 1)
            dist = float(row.get("trip_distance", 2.5))
            fare = float(row.get("fare_amount", 12.0))
            tot = float(row.get("total_amount", 15.5))
            pu_loc = int(row.get("PULocationID", random.randint(1, 263)))
            do_loc = int(row.get("DOLocationID", random.randint(1, 263)))
            pay = int(row.get("payment_type", 1))
        else:
            v_id = random.choice([1, 2])
            p_count = random.randint(1, 4)
            dist = round(random.uniform(0.5, 12.0), 2)
            fare = round(random.uniform(5.0, 45.0), 2)
            tot = round(fare + 3.5, 2)
            pu_loc = random.randint(1, 263)
            do_loc = random.randint(1, 263)
            pay = random.choice([1, 2])

        payload = {
            "trip_id": trip_id,
            "vendor_id": v_id,
            "pickup_datetime": now_iso,
            "dropoff_datetime": now_iso,
            "passenger_count": p_count,
            "trip_distance": dist,
            "fare_amount": fare,
            "total_amount": tot,
            "pickup_location_id": pu_loc,
            "dropoff_location_id": do_loc,
            "payment_type": pay
        }

        # Fault Injection: ~5% intentionally bad records
        is_bad = False
        if idx % 20 == 0:
            payload["fare_amount"] = -15.0  # Rule: No negative fares
            is_bad = True
        elif idx % 35 == 0:
            payload["passenger_count"] = 99  # Rule: Max 8 passengers
            is_bad = True
        elif idx % 50 == 0:
            payload["trip_distance"] = 0.0  # Rule: 0-distance high-fare
            payload["fare_amount"] = 80.0
            is_bad = True

        if is_bad:
            bad_trip_ids.append(trip_id)

        key = str(payload["pickup_location_id"]).encode('utf-8')
        val = json.dumps(payload).encode('utf-8')

        producer.produce(topic=topic, key=key, value=val, on_delivery=delivery_cb)
        producer.poll(0)
        time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start_time
    print(f"Replay complete: {max_events} messages in {elapsed:.2f}s ({max_events/elapsed:.1f} msg/s).")

    os.makedirs("tests", exist_ok=True)
    with open("tests/ground_truth.csv", "w") as f:
        f.write("trip_id\n" + "\n".join(bad_trip_ids))
    print(f"Stored {len(bad_trip_ids)} ground-truth anomaly IDs in tests/ground_truth.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="data/sample_trips.parquet")
    parser.add_argument("--max-events", type=int, default=3000)
    parser.add_argument("--rate", type=int, default=300)
    args = parser.parse_args()

    run_replay(args.file, args.max_events, args.rate)
