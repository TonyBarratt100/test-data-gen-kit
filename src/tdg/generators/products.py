from dataclasses import dataclass
import pandas as pd, numpy as np
from .base import RNG
from ..utils_distributions import zipf_popularity
CATEGORIES=["Electronics","Books","Home","Toys","Fashion","Beauty","Grocery","Outdoors"]
@dataclass
class ProductGen:
    rng:RNG
    def generate(self,n:int)->pd.DataFrame:
        f=self.rng.fake; probs=zipf_popularity(n); rows=[]
        for i in range(n):
            rows.append(dict(
                id=i+1, sku=f"SKU-{100000+i}", name=f.catch_phrase(),
                category=np.random.choice(CATEGORIES),
                price=round(np.random.lognormal(mean=3.2, sigma=0.5),2),
                stock=int(abs(np.random.normal(200,80))),
                popularity=probs[i],
                created_at=f.date_time_between(start_date="-3y", end_date="now")))
        df=pd.DataFrame(rows); df["popularity"]=df["popularity"]/df["popularity"].sum(); return df
