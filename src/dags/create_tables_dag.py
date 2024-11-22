from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Definição da DAG
with DAG(
    dag_id="create_tables_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="DAG para criar tabelas no banco de dados",
) as dag:

    # Criação da tabela Client
    create_client_table = PostgresOperator(
        task_id="create_client_table",
        postgres_conn_id="postgres_default",  # Configure sua conexão aqui
        sql="""
        CREATE TABLE IF NOT EXISTS Client (
            Client_id UUID PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT,
            gender VARCHAR(50),
            address VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(20)
        );
        """
    )

    # Criação da tabela Product
    create_product_table = PostgresOperator(
        task_id="create_product_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS Product (
            Product_id UUID PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price FLOAT NOT NULL,
            category VARCHAR(100),
            quantity VARCHAR(50)
        );
        """
    )

    # Criação da tabela Transaction
    create_transaction_table = PostgresOperator(
        task_id="create_transaction_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS Transaction (
            Transaction_id UUID PRIMARY KEY,
            client_id UUID,
            product_id UUID,
            payment_method VARCHAR(100),
            quantity INT NOT NULL,
            date DATE NOT NULL,
            FOREIGN KEY (client_id) REFERENCES Client(Client_id),
            FOREIGN KEY (product_id) REFERENCES Product(Product_id)
        );
        """
    )

    # Criação da tabela MlData
    create_mldata_table = PostgresOperator(
        task_id="create_mldata_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS MlData (
            id SERIAL PRIMARY KEY,
            marca VARCHAR(100),
            valor_total VARCHAR(100),
            valor_com_desconto VARCHAR(100),
            desconto VARCHAR(100)
        );
        """
    )

    # Ordem de execução das tarefas
    create_client_table >> create_product_table >> create_transaction_table >> create_mldata_table
