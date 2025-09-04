from faker import Faker
import pandas as pd
from typing import Sequence
def anonymize_columns(df:pd.DataFrame, columns:Sequence[str], seed:int=42, strategy:str="faker")->pd.DataFrame:
    fake=Faker(); fake.seed_instance(seed)
    df=df.copy()
    for col in columns:
        if col not in df.columns: continue
        if strategy=="faker":
            low=col.lower()
            if "email" in low: df[col]=[fake.unique.email() for _ in range(len(df))]
            elif "name" in low: df[col]=[fake.name() for _ in range(len(df))]
            elif "phone" in low: df[col]=[fake.phone_number() for _ in range(len(df))]
            elif "country" in low: df[col]=[fake.current_country() for _ in range(len(df))]
            else: df[col]=[fake.pystr(min_chars=8, max_chars=16) for _ in range(len(df))]
        else:
            df[col]=[str(abs(hash((col,i)))%(10**10)) for i in range(len(df))]
    return df
