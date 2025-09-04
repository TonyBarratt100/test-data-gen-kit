from dataclasses import dataclass
import pandas as pd, numpy as np
from .base import RNG
@dataclass
class UserGen:
    rng:RNG
    def generate(self,n:int)->pd.DataFrame:
        f=self.rng.fake; rows=[]
        for i in range(n):
            rows.append(dict(
                id=i+1, name=f.name(), email=f.unique.email(),
                phone=f.phone_number(), country=f.current_country(),
                created_at=f.date_time_between(start_date="-2y", end_date="now"),
                is_active=np.random.rand()>0.03))
        return pd.DataFrame(rows)
