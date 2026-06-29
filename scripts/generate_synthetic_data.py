import random
import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

# Configuration

SEED = 42
N_SHOPIFY_ORDERS = 500
N_AMAZON_ORDERS = 300
N_INVENTORY_RECORDS = 50
SAMPLE_SIZE = 20

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "data","samples")

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# Messiness injection helpers

def inject_nulls(df: pd.DataFrame, columns: list[str], null_rate: float = 0.03) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        mask = df.sample(frac=null_rate, random_state=SEED).index
        df.loc[mask, col] = None
    return df

def inject_duplicates(df: pd.DataFrame, duplicate_rate: float = 0.02) -> pd.DataFrame:
    n_dupes = max(1, int(len(df) * duplicate_rate))
    dupes = df.sample(n=n_dupes, random_state=SEED)
    return pd.concat([df, dupes], ignore_index=True)

def inject_late_arrivals(df: pd.DataFrame, date_column: str, late_rate: float = 0.02) -> pd.DataFrame:
    df = df.copy()
    n_late = max(1, int(len(df) * late_rate))
    late_idx = df.sample(n=n_late, random_state=SEED).index
    for idx in late_idx:
        original = pd.to_datetime(df.loc[idx, date_column])
        df.loc[idx, date_column] = original - timedelta(days=random.randint(1,3))
    return df

def inject_schema_drift(df: pd.DataFrame, drift_rate: float = 0.01) -> pd.DataFrame:
    df = df.copy()
    df["promo_code_applied"] = None
    n_drift = max(1, int(len(df) * drift_rate))
    drift_idx = df.sample(n=n_drift, random_state=SEED).index
    df.loc[drift_idx, "promo_code_applied"] = [fake.bothify(text="PROMO-####") for _ in range(n_drift)]
    return df

# Source generators

def generate_shopify_orders(n: int) -> pd.DataFrame:
    statuses = ["placed", "shipped", "delivered", "cancelled"]
    rows = []
    for i in range(n):
        order_date = fake.date_between(start_date="-90d", end_date="today")
        rows.append({
            "order_id": f"SHOP-{i:06d}",
            "customer_id": f"CUST-{random.randint(1, 150):05d}",
            "product_id": f"PROD-{random.randint(1, 80):04d}",
            "order_date": order_date,
            "quantity": random.randint(1,5),
            "revenue": round(random.uniform(10, 500), 2),
            "order_status": random.choice(statuses),
        })
    df = pd.DataFrame(rows)
    df = inject_nulls(df, columns=["order_status"], null_rate=0.03)
    df = inject_duplicates(df, duplicate_rate=0.02)
    df = inject_late_arrivals(df, date_column="order_date", late_rate=0.02)
    df = inject_schema_drift(df, drift_rate=0.01)
    return df

def generate_amazon_orders(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        order_timestamp = fake.date_time_between(start_date="-90d", end_date="now")
        rows.append({
            "marketplace_order_id": f"AMZN-{i:06d}",
            "customer_id": f"CUST-{random.randint(1,150):05d}",
            "sku": f"PROD-{random.randint(1,80):04d}",
            "quantity": random.randint(1,5),
            "revenue": round(random.uniform(10,500),2),
            "order_timestamp": order_timestamp,
        })
    df = pd.DataFrame(rows)
    df = inject_nulls(df, columns=["customer_id"], null_rate=0.03)
    df = inject_duplicates(df, duplicate_rate=0.02)
    df = inject_late_arrivals(df, date_column="order_timestamp", late_rate=0.02)
    return df

def generate_inventory_feed(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "inventory_id": f"INV-{i:05d}",
            "product_id": f"PROD-{random.randint(1,80):04d}",
            "warehouse_id": f"WH-{random.randint(1,5):02d}",
            "quantity_available": random.randint(0,1000),
            "quantity_reserved": random.randint(0,100),
            "last_updated": fake.date_time_between(start_date="-7d",end_date="now"),
        })
    df = pd.DataFrame(rows)
    df = inject_nulls(df, columns=["quantity_reserved"], null_rate=0.03)
    return df

# Main

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    datasets = {
        "shopify_orders": generate_shopify_orders(N_SHOPIFY_ORDERS),
        "amazon_orders": generate_amazon_orders(N_AMAZON_ORDERS),
        "inventory_feed": generate_inventory_feed(N_INVENTORY_RECORDS),
    }

    for name, df in datasets.items():
        full_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        sample_path = os.path.join(SAMPLE_DIR, f"{name}_sample.csv")

        df.to_csv(full_path, index=False)
        df.head(SAMPLE_SIZE).to_csv(sample_path, index=False)

        print(f"{name}: {len(df)} rows -> {full_path}")
        print(f"{name}: {min(SAMPLE_SIZE, len(df))} rows -> {sample_path}")

    print("\nDone. Full datasets are gitignored; samples are tracked for review.")

if __name__ == "__main__":
    main()  