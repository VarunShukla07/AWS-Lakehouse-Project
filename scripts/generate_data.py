from faker import Faker
import psycopg2
from datetime import datetime
import uuid
import random
from dotenv import load_dotenv
import os

load_dotenv()

fake = Faker()

# UPDATE THESE with your actual values
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def generate_users(n=10000):
    conn = get_connection()
    cur = conn.cursor()
    
    print(f"Generating {n} users...")
    for i in range(n):
        user_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO users (user_id, name, email, address, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, fake.name(), fake.email(), fake.address(), 'active'))
        
        if i % 1000 == 0 and i != 0:
            print(f"Progress: {i}")
            conn.commit()
            print(f"  {i} users inserted")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Users done!")

def generate_products(n=1000):
    conn = get_connection()
    cur = conn.cursor()
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    
    print(f"Generating {n} products...")
    for i in range(n):
        product_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO products (product_id, name, category, price, stock)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            product_id,
            fake.word().title() + " " + fake.word().title(),
            random.choice(categories),
            round(random.uniform(10, 500), 2),
            random.randint(0, 1000)
        ))
        
        if i % 500 == 0 and i != 0:
            print(f"Progress: {i}")
            conn.commit()
    
    conn.commit()
    cur.close()
    conn.close()
    print("Products done!")

def generate_orders(n=50000):
    conn = get_connection()
    cur = conn.cursor()
    
    # Get all user_ids and product_ids
    cur.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()
    
    print(f"Generating {n} orders...")
    for i in range(n):
        order_id = str(uuid.uuid4())
        user_id = random.choice(user_ids)
        total = round(random.uniform(20, 1000), 2)
        
        cur.execute("""
            INSERT INTO orders (order_id, user_id, total_amount, status)
            VALUES (%s, %s, %s, %s)
        """, (order_id, user_id, total, random.choice(['pending', 'completed', 'cancelled'])))
        
        # 1-5 items per order
        for _ in range(random.randint(1, 5)):
            product = random.choice(products)
            item_id = str(uuid.uuid4())
            qty = random.randint(1, 3)
            cur.execute("""
                INSERT INTO order_items (item_id, order_id, product_id, quantity, price_at_time)
                VALUES (%s, %s, %s, %s, %s)
            """, (item_id, order_id, product[0], qty, product[1]))
        
        if i % 5000 == 0 and i != 0:
            print(f"Progress: {i}")
            conn.commit()
            print(f"  {i} orders inserted")
    
    conn.commit()
    cur.close()
    conn.close()
    print("Orders done!")

if __name__ == "__main__":
    generate_users(100)
    generate_products(100)
    generate_orders(500)
    print("All data generated!")