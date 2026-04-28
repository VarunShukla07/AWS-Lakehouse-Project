import psycopg2
import random
import time
from dotenv import load_dotenv
import os

load_dotenv()

# UPDATE THESE with your actual values
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
cur = conn.cursor()

print("Simulating changes for CDC...")

# Update 10% of users (simulate churn)
cur.execute("SELECT user_id FROM users WHERE status = 'active' LIMIT 10")
users = [row[0] for row in cur.fetchall()]

for user_id in users:
    new_status = random.choice(['inactive', 'premium', 'suspended'])
    cur.execute("UPDATE users SET status = %s WHERE user_id = %s", (new_status, user_id))

print(f"Updated {len(users)} users")

# Delete some old cancelled orders
cur.execute("""
    WITH cte AS (
        SELECT order_id FROM orders WHERE status = 'cancelled' LIMIT 500
    )
    DELETE FROM order_items 
    WHERE order_id IN (SELECT order_id FROM cte)
""")
cur.execute("""
    WITH cte AS (
        SELECT order_id FROM orders WHERE status = 'cancelled' LIMIT 50
    )
    DELETE FROM orders 
    WHERE order_id IN (SELECT order_id FROM cte)
""")
print("Deleted cancelled orders")

conn.commit()
cur.close()
conn.close()
print("Changes committed! CDC should capture these.")