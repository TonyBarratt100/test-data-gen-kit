from .config import API_BASE
import httpx
from typing import Iterable, Mapping, Any
from tqdm import tqdm
def _ensure_slash(p:str)->str: return p if p.startswith('/') else '/'+p
def post_rows(endpoint_base:str|None, path:str, rows:Iterable[Mapping[str,Any]], timeout:float=5.0):
    base=(endpoint_base or API_BASE).rstrip('/'); url=base+_ensure_slash(path)
    with httpx.Client(timeout=timeout) as client:
        for row in tqdm(list(rows), desc=f"POST {path}"):
            r=client.post(url, json=row); r.raise_for_status()
