from fastapi import FastAPI, Request
import json

app = FastAPI()

@app.post("/orders")
async def receive_order(req: Request):
    order = await req.json()
    with open("orders_received.json", "a") as f:
        f.write(json.dumps(order) + "\n")
    return {"status": "Order received"}