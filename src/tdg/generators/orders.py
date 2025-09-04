from dataclasses import dataclass
import pandas as pd, numpy as np
from .base import RNG
@dataclass
class OrderGen:
    rng:RNG
    def generate(self,n:int, users:pd.DataFrame, products:pd.DataFrame)->pd.DataFrame:
        f=self.rng.fake
        user_ids=users["id"].tolist(); product_ids=products["id"].tolist()
        probs=products["popularity"].values; probs=probs/probs.sum()
        rows=[]
        for i in range(n):
            uid=int(np.random.choice(user_ids))
            pid=int(np.random.choice(product_ids, p=probs))
            qty=int(max(1, np.random.geometric(p=0.4)))
            price=float(products.loc[products['id']==pid,'price'].iloc[0])
            rows.append(dict(
                id=i+1, user_id=uid, product_id=pid, quantity=qty,
                total=round(price*qty,2),
                status=np.random.choice(["created","paid","shipped","cancelled"], p=[0.2,0.5,0.27,0.03]),
                created_at=f.date_time_between(start_date="-18mo", end_date="now")))
        return pd.DataFrame(rows)
