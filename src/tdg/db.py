from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import DB_URL
DDL=[
"""CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY,name TEXT,email TEXT UNIQUE,phone TEXT,country TEXT,created_at TIMESTAMPTZ,is_active BOOLEAN);""",
"""CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY,sku TEXT UNIQUE,name TEXT,category TEXT,price NUMERIC,stock INT,popularity DOUBLE PRECISION,created_at TIMESTAMPTZ);""",
"""CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY,user_id INT REFERENCES users(id) ON DELETE CASCADE,product_id INT REFERENCES products(id) ON DELETE CASCADE,quantity INT,total NUMERIC,status TEXT,created_at TIMESTAMPTZ);""",
"""CREATE TABLE IF NOT EXISTS reviews (id INT PRIMARY KEY,user_id INT REFERENCES users(id) ON DELETE CASCADE,product_id INT REFERENCES products(id) ON DELETE CASCADE,rating INT,title TEXT,body TEXT,created_at TIMESTAMPTZ);"""
]
def get_engine(url:str|None=None)->Engine: return create_engine(url or DB_URL, future=True)
def ensure_tables(engine:Engine):
    with engine.begin() as c:
        for stmt in DDL: c.execute(text(stmt))
def truncate_tables(engine:Engine,tables:list[str]):
    with engine.begin() as c:
        for t in tables: c.execute(text(f'TRUNCATE TABLE {t} RESTART IDENTITY CASCADE'))
def bulk_insert(engine:Engine, table:str, df):
    df.to_sql(table, con=engine, if_exists='append', index=False, method='multi')
