from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging

default_args = {
    'owner': 'vinicius',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DBT_PROJECT_PATH = '/home/vinicius/Area_de_Trabalho/dev/Lighthouse/adventure_works_dbt'  # Sem espaços
ENV_PATH = f"{DBT_PROJECT_PATH}/.env"

def load_env_vars(**context):
    try:
        if not os.path.exists(ENV_PATH):
            raise FileNotFoundError(f"Arquivo .env não encontrado em: {ENV_PATH}")
        # Lê o arquivo .env e processa as variáveis
        with open(ENV_PATH, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if 'export' in line:
                        line = line.replace('export', '').strip()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip().strip("'").strip('"')
                        os.environ[key.strip()] = value
        # Adicionando a variável necessária, caso não exista
        if 'DBT_SNOWFLAKE_ACCOUNT_ID' not in os.environ:
            raise EnvironmentError("A variável de ambiente 'DBT_SNOWFLAKE_ACCOUNT_ID' não foi fornecida.")
        logging.info("Variáveis de ambiente carregadas com sucesso")
        logging.info(f"DBT_SNOWFLAKE_ACCOUNT_ID: {os.environ.get('DBT_SNOWFLAKE_ACCOUNT_ID')}")
        logging.info(f"DBT_TARGET: {os.environ.get('DBT_TARGET')}")
        logging.info(f"DBT_USER: {os.environ.get('DBT_USER')}")
        logging.info(f"DBT_PASSWORD: {os.environ.get('DBT_PASSWORD')}")
    except Exception as e:
        logging.error(f"Erro ao carregar variáveis de ambiente: {str(e)}")
        raise

dag = DAG(
    'dbt_daily_build',
    default_args=default_args,
    description='Executa dbt build diariamente às 2:00',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 1, 19),
    catchup=False,
    tags=['dbt', 'production'],
)

# Task para carregar variáveis de ambiente
load_env = PythonOperator(
    task_id='load_env',
    python_callable=load_env_vars,
    provide_context=True,
    dag=dag,
)

# Variáveis de ambiente explicitamente necessárias para o dbt
dbt_env = {
    "DBT_PROFILES_DIR": DBT_PROJECT_PATH,
    **{key: os.environ[key] for key in ["DBT_TARGET", "DBT_USER", "DBT_PASSWORD"] if key in os.environ},
}

# Task para instalar dependências do dbt
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f"""
    cd {DBT_PROJECT_PATH} && \
    /home/vinicius/Area_de_Trabalho/dev/Lighthouse/adventure_works_airflow/airflow-venv/bin/dbt deps --debug
    """,
    env=dbt_env,
    dag=dag,
)

# Task para rodar o dbt build
dbt_run = BashOperator(
    task_id='dbt_run_prod',
    bash_command=f"""
    cd {DBT_PROJECT_PATH} && \
    /home/vinicius/Area_de_Trabalho/dev/Lighthouse/adventure_works_airflow/airflow-venv/bin/dbt run --target prod --debug > dbt_run.log 2>&1
    """,
    env=dbt_env,
    dag=dag,
)

# Task para rodar o dbt test
dbt_test = BashOperator(
    task_id='dbt_test_prod',
    bash_command=f"""
    cd {DBT_PROJECT_PATH} && \
    /home/vinicius/Area_de_Trabalho/dev/Lighthouse/adventure_works_airflow/airflow-venv/bin/dbt test --target prod --debug > dbt_test.log 2>&1
    """,
    env=dbt_env,
    dag=dag,
)

# Ordem de execução das tasks
load_env >> dbt_deps >> dbt_run >> dbt_test
