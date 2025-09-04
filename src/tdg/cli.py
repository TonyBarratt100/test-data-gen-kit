import typer, os, pandas as pd
from typing import Optional, List
from .config import DEFAULT_USERS, DEFAULT_PRODUCTS, DEFAULT_ORDERS, DEFAULT_REVIEWS, SEED, OUT_DIR
from .generators.base import RNG
from .generators.users import UserGen
from .generators.products import ProductGen
from .generators.orders import OrderGen
from .generators.reviews import ReviewGen
from .db import get_engine, ensure_tables, truncate_tables, bulk_insert
from .anonymize import anonymize_columns
from .api_client import post_rows

app=typer.Typer(help="Test Data Gen Kit CLI")

def _generate_all(users:int, products:int, orders:int, reviews:int, seed:int):
    rng=RNG(seed=seed)
    u=UserGen(rng).generate(users)
    p=ProductGen(rng).generate(products)
    o=OrderGen(rng).generate(orders, u, p)
    r=ReviewGen(rng).generate(reviews, u, p, o)
    return u,p,o,r

@app.command("generate")
def generate(users:int=DEFAULT_USERS, products:int=DEFAULT_PRODUCTS, orders:int=DEFAULT_ORDERS, reviews:int=DEFAULT_REVIEWS,
             out:str=OUT_DIR, format:str=typer.Option("csv", help="csv or json"), seed:int=SEED):
    os.makedirs(out, exist_ok=True)
    u,p,o,r=_generate_all(users,products,orders,reviews,seed)
    if format=="csv":
        u.to_csv(os.path.join(out,"users.csv"), index=False)
        p.to_csv(os.path.join(out,"products.csv"), index=False)
        o.to_csv(os.path.join(out,"orders.csv"), index=False)
        r.to_csv(os.path.join(out,"reviews.csv"), index=False)
    elif format=="json":
        u.to_json(os.path.join(out,"users.json"), orient="records", lines=True)
        p.to_json(os.path.join(out,"products.json"), orient="records", lines=True)
        o.to_json(os.path.join(out,"orders.json"), orient="records", lines=True)
        r.to_json(os.path.join(out,"reviews.json"), orient="records", lines=True)
    else:
        raise typer.BadParameter("format must be csv or json")
    typer.echo(f"Wrote {format.upper()} to {out}")

@app.command("seed-postgres")
def seed_postgres(users:int=DEFAULT_USERS, products:int=DEFAULT_PRODUCTS, orders:int=DEFAULT_ORDERS, reviews:int=DEFAULT_REVIEWS,
                  db_url:Optional[str]=typer.Option(None,"--db-url"), truncate:bool=typer.Option(False,"--truncate"),
                  create:bool=typer.Option(True,"--create"), seed:int=SEED):
    eng=get_engine(db_url)
    if create: ensure_tables(eng)
    if truncate: truncate_tables(eng, ["reviews","orders","products","users"])
    u,p,o,r=_generate_all(users,products,orders,reviews,seed)
    for name,df in [("users",u),("products",p),("orders",o),("reviews",r)]:
        bulk_insert(eng, name, df)
    typer.echo("Seeded Postgres successfully.")

@app.command("call-api")
def call_api(users:int=50, orders:int=120, reviews:int=100, api_base:Optional[str]=None,
             user_path:str="/users", order_path:str="/orders", review_path:str="/reviews", seed:int=SEED):
    u,p,o,r=_generate_all(users, products=80, orders=orders, reviews=reviews, seed=seed)
    post_rows(api_base, user_path, u.to_dict(orient="records"))
    post_rows(api_base, order_path, o.to_dict(orient="records"))
    post_rows(api_base, review_path, r.to_dict(orient="records"))
    typer.echo("API calls completed.")

@app.command("anonymize")
def anonymize(input:str, output:str, columns:List[str], strategy:str="faker", seed:int=SEED):
    if len(columns)==1 and "," in columns[0]: cols=[c.strip() for c in columns[0].split(",")]
    else: cols=columns
    df=pd.read_csv(input); out=anonymize_columns(df, cols, seed=seed, strategy=strategy)
    out.to_csv(output, index=False); typer.echo(f"Wrote anonymized CSV to {output}")

if __name__=="__main__": app()
