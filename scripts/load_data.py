import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL connection configuration
conn = psycopg2.connect(
    dbname="ecommerce",
    user="postgres",
    password="manoj",  # 🔁 Replace with your PostgreSQL password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Define CSVs and corresponding table details
csv_table_map = [
    {
        "file": "../data/distribution_centers.csv",
        "table": "distribution_centers",
        "columns": ["id", "name", "latitude", "longitude"]
    },
    {
        "file": "../data/products.csv",
        "table": "products",
        "columns": ["id", "cost", "category", "name", "brand", "retail_price", "department", "sku", "distribution_center_id"]
    },
    {
        "file": "../data/inventory_items.csv",
        "table": "inventory_items",
        "columns": ["id", "product_id", "created_at", "sold_at", "cost", "product_category", "product_name", "product_brand", "product_retail_price", "product_department", "product_sku", "product_distribution_center_id"]
    },
    {
        "file": "../data/users.csv",
        "table": "users",
        "columns": ["id", "first_name", "last_name", "email", "age", "gender", "state", "street_address", "postal_code", "city", "country", "latitude", "longitude", "traffic_source", "created_at"]
    },
    {
        "file": "../data/orders.csv",
        "table": "orders",
        "columns": ["order_id", "user_id", "status", "gender", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"]
    },
    {
        "file": "../data/order_items.csv",
        "table": "order_items",
        "columns": ["id", "order_id", "user_id", "product_id", "inventory_item_id", "status", "created_at", "shipped_at", "delivered_at", "returned_at"]
    }
]


# Load and insert each CSV into corresponding table
def load_csv_to_postgres(file_path, table_name, columns):
    df = pd.read_csv(file_path)
    df = df.where(pd.notnull(df), None)  # Replace NaN with None for SQL
    values = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    execute_values(cur, sql, values)
    print(f"✅ Inserted into {table_name}: {len(values)} rows")

for entry in csv_table_map:
    load_csv_to_postgres(entry["file"], entry["table"], entry["columns"])

conn.commit()
cur.close()
conn.close()
print("\n🎉 All CSVs loaded into PostgreSQL database!")
