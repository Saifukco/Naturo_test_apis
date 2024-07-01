from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Dummy database
items_db = [
    {"id": 1, "name": "Item 1", "description": "Description of item 1", "price": 10.0, "tax": 0.5},
    {"id": 2, "name": "Item 2", "description": "Description of item 2", "price": 20.0, "tax": 1.0},
]

# Pydantic model for item
class Item(BaseModel):
    id: int
    name: str
    description: str = None
    price: float
    tax: float = None

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI application"}

@app.get("/items", response_model=List[Item])
def get_items():
    return items_db

@app.get("/items/{item_id}", response_model=Item)
def get_item(item_id: int):
    for item in items_db:
        if item["id"] == item_id:
            return item
    raise HTTPException(status_code=404, detail="Item not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
