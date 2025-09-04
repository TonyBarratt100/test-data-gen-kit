from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import uvicorn

app = FastAPI(title="TDG Mock API")

# --- Schemas matching minimal payloads sent by tdg.cli call-api ---
class User(BaseModel):
    id: int
    name: str
    email: str
    phone: Optional[str] = None
    country: Optional[str] = None
    created_at: Optional[str] = None
    is_active: Optional[bool] = None

class Order(BaseModel):
    id: int
    user_id: int
    product_id: int
    quantity: int
    total: float
    status: str
    created_at: Optional[str] = None

class Review(BaseModel):
    id: int
    user_id: int
    product_id: int
    rating: int
    title: str
    body: str
    created_at: Optional[str] = None

# --- In-memory store ---
DB = {"users": [], "orders": [], "reviews": []}

@app.post("/users")
def create_user(u: User):
    DB["users"].append(u.model_dump())
    return {"ok": True, "count": len(DB["users"])}

@app.post("/orders")
def create_order(o: Order):
    DB["orders"].append(o.model_dump())
    return {"ok": True, "count": len(DB["orders"])}

@app.post("/reviews")
def create_review(r: Review):
    DB["reviews"].append(r.model_dump())
    return {"ok": True, "count": len(DB["reviews"])}

@app.get("/stats")
def stats():
    return {k: len(v) for k, v in DB.items()}

@app.post("/reset")
def reset():
    for k in DB: DB[k].clear()
    return {"ok": True}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
