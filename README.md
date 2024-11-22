# pipeline-scraping

## Para Executar o Projeto
Primeiro, clone o repositorio
```bash
ssh
git clone git@github.com:Araldi42/pipeline-scraping.git
```
Segundamente, vamos configurar o projeto para ser executado:
```bash
# Crie um .env e adicione as seguintes variaveis:
AIRFLOW_UID=50000
DB_USER='airflow'
DB_PASSWORD='airflow'
DB_NAME='airflow'
DB_HOST='pipeline-scraping-postgres-1'
DB_PORT='5432'
#Nota: DB_HOST pode mudar de acordo com o nome da pasta root do projeto que usar.

# Certifique-se de ter Docker instalado em sua máquina de execução

#execute
docker compose up airflow-init
docker compose up -d

```
Abra a UI do Airflow disponível em http://localhost:8080/ 
Senha padrão é:
```bash
User = airflow
Pass = airflow
```

```bash
#navegue em Admin > Connections > create Connection
# Crie uma conexão com os parâmetros:
 # Connection Id: 
    postgres_default
 # Connection Type:
    Postgres
 #  Host:
    pipeline-scraping-postgres-1 # Muda conforme diretorio root do projeto, para confirmar, abra um terminal e execute 
    docker network inspect pipeline-scraping_default
    # Procure pelo nome do conteiner postgres
 # Database
    airflow
 # Login
    airflow
 # Password
    airflow
 # Port
    5432
# Save
```
Por fim, vamos iniciar a api que nos retorna os dados
```bash
# Abra um terminal e va para a pasta root do projeto
# Crie uma venv python
# Exemplo usando conda/anaconda/miniconda:
conda create -n <venv> python=3.11

conda activate <venv>

# Instale as libs do projeto:
pip install -r requirements.txt

# Inicie a api
python src/api/main.py
```
Tudas as configurações estão prontas.
Na UI do airflow, primeiro, execute 'create_tables_dag' e depois execute 'etl_scraping.py'