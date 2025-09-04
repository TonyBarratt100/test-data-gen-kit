from dataclasses import dataclass
import pandas as pd, numpy as np, random
from .base import RNG
TITLES=["Exceeded expectations","Solid value","Not as described","Five stars","Would buy again","Quality could be better","Fast shipping","Amazing product"]
@dataclass
class ReviewGen:
    rng:RNG
    def generate(self,n:int, users:pd.DataFrame, products:pd.DataFrame, orders:pd.DataFrame)->pd.DataFrame:
        f=self.rng.fake
        pop=products.set_index("id")["popularity"]
        product_ids=products["id"].tolist(); user_ids=users["id"].tolist()
        order_pairs=orders[["user_id","product_id"]].drop_duplicates().values.tolist()
        rows=[]
        for i in range(n):
            if order_pairs and np.random.rand()<0.7:
                uid,pid=random.choice(order_pairs)
            else:
                uid=int(np.random.choice(user_ids)); pid=int(np.random.choice(product_ids))
            base=3.6 + float(pop.get(pid,0.0))*2.0
            rating=int(np.clip(np.round(np.random.normal(loc=base, scale=0.9)),1,5))
            rows.append(dict(
                id=i+1, user_id=int(uid), product_id=int(pid), rating=rating,
                title=np.random.choice(TITLES), body=f.paragraph(nb_sentences=3),
                created_at=f.date_time_between(start_date="-12mo", end_date="now")))
        return pd.DataFrame(rows)
