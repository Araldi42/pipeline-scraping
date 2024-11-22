import os
from fastapi import FastAPI, HTTPException
from transaction_generator import transactionGenerator
from client_generator import clientGenerator
from product_generator import productGenerator
import uvicorn
import asyncio
import logging

CURRENT_PATH = os.path.dirname(__file__)
logging.basicConfig(filename=f"{CURRENT_PATH}/../airflow/logs/errors.log", level=logging.ERROR)

app = FastAPI()

@app.get("/api/clients")
async def generate_clients(num_records: int):
    try:
        client_gen = clientGenerator(num_records)
        client_data = client_gen.generate()
        return client_data
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/api/products")
async def generate_products(num_records: int):
    try:
        product_gen = productGenerator(num_records)
        product_data = product_gen.generate()
        return product_data
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/api/transactions")
async def generate_transactions(num_records: int):
    try:
        client_gen = clientGenerator(num_records)
        product_gen = productGenerator(num_records)
        client_data = client_gen.generate()
        product_data = product_gen.generate()
        # Extract client_ids and product_ids
        client_ids = [client['client_id'] for client in client_data]
        product_ids = [product['product_id'] for product in product_data]     
        # Generate transactions
        transaction_gen = transactionGenerator(num_records, client_ids, product_ids)
        transaction_data = transaction_gen.generate()
        return transaction_data
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def main():
    config = uvicorn.Config("main:app", port=8000, host='0.0.0.0', log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())

