import os
import sys
import requests
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Param
from airflow.exceptions import AirflowFailException

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.database import Database
from utils.api import get_clients_data, get_products_data, get_transactions_data
from utils.scraping import gerar_url, extrair_informacoes, treat_data, validate_schema, normalize_data

logger = logging.getLogger(__name__)

db = Database()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_retry_delay': timedelta(minutes=60),
}


dag = DAG(
    'mercado_livre_scraping',
    default_args=default_args,
    description='Extrai informações de relógios do Mercado Livre',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    params = {
        'numero_paginas_scraping': Param(3, type='integer'),
        'numero_registros_clientes': Param(1000, type='integer'),
        'numero_registros_produtos': Param(1000, type='integer'),
        'numero_registros_transacoes': Param(1000, type='integer'),
    }
)


def executar_scraping(**context):
    #Parametro configurado, selecione o numero de paginas que deseja extrair dados
    numero_paginas = context['params']['numero_paginas_scraping']
    for pagina in range(1, numero_paginas + 1):
        # Gerar a URL da página
        url = gerar_url(pagina)
        try:
            # Enviar uma requisição GET para a página
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao acessar a página {pagina}: {e}")
            raise AirflowFailException(f"Falha ao acessar a página {pagina}") from e
        # Extrair as informações dos produtos
        soup = BeautifulSoup(response.content, 'html.parser')
        # Encontrar todos os itens da lista
        itens = soup.find_all('li', class_='ui-search-layout__item')
        list_info = []
        # Iterar sobre os itens
        for item in itens:
            # Encontrar o conteúdo da div
            conteudo_div = item.find('div', class_='ui-search-result__wrapper')
            # Extrair as informações do conteúdo
            if conteudo_div:
                try:
                    # Extrair o texto do conteúdo
                    texto = conteudo_div.get_text(strip=True)
                    # Extrair as informações do texto
                    info = extrair_informacoes(texto)
                    if validate_schema(info):
                        list_info.append(info)
                    else:
                        logger.warning(f"Esquema inválido: {info}")
                        continue
                except Exception as e:
                    logger.error(f"Erro ao extrair informações do item: {e}")
                    raise AirflowFailException(f"Falha ao extrair informações do item") from e
        try:
            # Inserir as informações no banco de dados
            columns = ['marca', 'valor_total', 'valor_com_desconto', 'desconto']
            list_info = treat_data(list_info)
            list_info = normalize_data(list_info)
            for info in list_info:
                # Inserir os valores no banco de dados
                values = [info['marca'], info['valor_total'], info['valor_com_desconto'], info['desconto']]
                db.insert('mldata', columns, values)
        except Exception as e:
            logger.error(f"Erro ao inserir informações no banco de dados: {e}")
            raise AirflowFailException(f"Falha ao inserir informações no banco de dados") from e


def request_clients_data_from_api(**context):
    # Parametro configurado, selecione o numero de registros que deseja extrair dados
    numero_registros = context['params']['numero_registros_clientes']
    try:
        clients_data = get_clients_data(numero_registros)
    except Exception as e:
        logger.error(f"Erro ao extrair dados dos clientes: {e}")
        raise AirflowFailException(f"Falha ao extrair dados dos clientes") from e
    # Colunas da tabela
    columns = ['client_id', 'name', 'age','gender','address','email','phone_number']
    # Iterar sobre os dados dos clientes
    for data in clients_data:
        try:
            # Valores a serem inseridos
            values = [data['client_id'], data['name'], data['age'], data['gender'], data['address'], data['email'], data['phone_number']]
            db.insert('client', columns, values)
        except Exception as e:
            logger.error(f"Erro ao inserir dados dos clientes no banco de dados: {e}")
            raise AirflowFailException(f"Falha ao inserir dados dos clientes no banco de dados") from e


def request_products_data_from_api(**context):
    # Parametro configurado, selecione o numero de registros que deseja extrair dados
    numero_registros = context['params']['numero_registros_produtos']
    try:
        products_data = get_products_data(numero_registros)
    except Exception as e:
        logger.error(f"Erro ao extrair dados dos produtos: {e}")
        raise AirflowFailException(f"Falha ao extrair dados dos produtos") from e
    # Colunas da tabela
    columns = ['product_id', 'name', 'price','category','quantity']
    # Iterar sobre os dados dos produtos
    for data in products_data:
        # Valores a serem inseridos
        try:
            values = [data['product_id'], data['name'], data['price'], data['category'], data['quantity']]
            db.insert('product', columns, values)
        except Exception as e:
            logger.error(f"Erro ao inserir dados dos produtos no banco de dados: {e}")
            raise AirflowFailException(f"Falha ao inserir dados dos produtos no banco de dados") from e

def request_transactions_data_from_api(**context):
    # Parametro configurado, selecione o numero de registros que deseja extrair dados
    numero_registros = context['params']['numero_registros_transacoes']
    try:
        transactions_data = get_transactions_data(numero_registros)
    except Exception as e:
        logger.error(f"Erro ao extrair dados das transações: {e}")
        raise AirflowFailException(f"Falha ao extrair dados das transações") from e
    # Colunas da tabela
    columns = ['transaction_id', 'client_id', 'product_id','payment_method','quantity','date']
    # Iterar sobre os dados das transações
    for data in transactions_data:
        try:
            # Valores a serem inseridos
            values = [data['transaction_id'], data['client_id'], data['product_id'], data['payment_method'], data['quantity'], data['date']]
            db.insert('transaction', columns, values)
        except Exception as e:
            logger.error(f"Erro ao inserir dados das transações no banco de dados: {e}")
            raise AirflowFailException(f"Falha ao inserir dados das transações no banco de dados") from e


scraping_task = PythonOperator(
    task_id='executar_scraping',
    python_callable=executar_scraping,
    dag=dag,
)

request_clients_data_task = PythonOperator(
    task_id='request_clients_data_from_api',
    python_callable=request_clients_data_from_api,
    dag=dag,
)

request_products_data_task = PythonOperator(
    task_id='request_products_data_from_api',
    python_callable=request_products_data_from_api,
    dag=dag,
)

request_transactions_data_task = PythonOperator(
    task_id='request_transactions_data_from_api',
    python_callable=request_transactions_data_from_api,
    dag=dag,
)

scraping_task >> request_clients_data_task >> request_products_data_task >> request_transactions_data_task
