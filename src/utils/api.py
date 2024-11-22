import requests

def get_clients_data(quantity):
    url = f'http://host.docker.internal:8000/api/clients?num_records={quantity}'
    response = requests.get(url)
    return response.json()

def get_products_data(quantity):
    url = f'http://host.docker.internal:8000/api/products?num_records={quantity}'
    response = requests.get(url)
    return response.json()

def get_transactions_data(quantity):
    url = f'http://host.docker.internal:8000/api/transactions?num_records={quantity}'
    response = requests.get(url)
    return response.json()

