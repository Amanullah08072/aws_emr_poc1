import csv
import os
import random
from datetime import datetime, timedelta

FILE_NAME = "raw_clickstream_data/user_activity_logs.csv"
TARGET_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # Strict 5 GB boundary
BATCH_SIZE = 50000

print(f"🚀 Generating a massive 5 GB Raw Clickstream CSV Dataset...")
os.makedirs(os.path.dirname(FILE_NAME), exist_ok=True)

header = [
    "click_id", "session_id", "user_id", "event_timestamp", "page_url", 
    "product_category", "action_type", "device_type", "ip_address", 
    "session_duration_sec", "purchase_amount", "http_status"
]

categories = ["Electronics", "Apparel", "Home_Kitchen", "Beauty", "Automotive", "Books"]
actions = ["view_page", "search_product", "add_to_cart", "click_ad", "checkout_complete"]
devices = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_safari", "smart_tv"]

base_time = datetime(2026, 1, 1)

with open(FILE_NAME, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    
    total_rows = 0
    while os.path.getsize(FILE_NAME) < TARGET_SIZE_BYTES:
        rows = []
        for _ in range(BATCH_SIZE):
            # Normal timeline simulation
            days_offset = random.randint(0, 90)
            log_time = base_time + timedelta(days=days_offset, hours=random.randint(0, 23), minutes=random.randint(0, 59))
            
            # INTENTIONAL DIRTY DATA INJECTION: Simulated device clock desync (Slide 10 matching)
            if random.random() < 0.001: 
                timestamp_str = f"{random.choice([1970, 2099])}-01-01 00:00:00"
                category = "CORRUPTED_EVENT"
            else:
                timestamp_str = log_time.strftime("%Y-%m-%d %H:%M:%S")
                category = random.choice(categories)
                
            click_id = f"CLK-{random.randint(10000000, 99999999)}"
            session_id = f"SES-{random.randint(100000, 999999)}"
            user_id = f"USR-{random.randint(500000, 600000)}"
            action = random.choice(actions)
            
            # Populate transaction values if they actually check out
            purchase = round(random.uniform(10.99, 899.99), 2) if action == "checkout_complete" else 0.00
            status = random.choice([200, 200, 200, 404, 500])
            
            rows.append([
                click_id, session_id, user_id, timestamp_str, f"/products/{category.lower()}",
                category, action, random.choice(devices), f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
                random.randint(2, 600), purchase, status
            ])
            
        writer.writerows(rows)
        total_rows += BATCH_SIZE
        current_size = os.path.getsize(FILE_NAME) / (1024 ** 3)
        print(f"⚡ Current CSV Footprint: {current_size:.2f} GB generated ({total_rows} rows)...", end="\r")

print(f"\n\n✅ Done! File size successfully hit: {os.path.getsize(FILE_NAME) / (1024 ** 3):.2f} GB")