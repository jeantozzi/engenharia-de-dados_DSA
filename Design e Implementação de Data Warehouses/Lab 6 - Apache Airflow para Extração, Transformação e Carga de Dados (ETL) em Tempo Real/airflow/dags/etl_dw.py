# Lab 6 - Job ETL - Versão 5

# Imports
import csv
import airflow
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# Argumentos
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Cria a DAG
# https://crontab.guru/
dag_lab6_dsa = DAG(dag_id = "lab6_final",
                   default_args = default_args,
                   schedule_interval = '0 0 * * *',
                   dagrun_timeout = timedelta(minutes = 60),
                   description = 'Job ETL de Carga no DW com Airflow',
                   start_date = airflow.utils.dates.days_ago(1)
)

##### Tabela de Clientes #####

def func_carrega_dados_clientes(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_cli = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_cli = "INSERT INTO lab6.DIM_CLIENTE (%s) VALUES (%s)" % (','.join(dados_cli.keys()), ','.join([item for item in dados_cli.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_clientes_' + str(i), 
                                                 sql = sql_query_cli, 
                                                 params = (dados_cli), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_clientes = PythonOperator(
        task_id = 'tarefa_carrega_dados_clientes',
        python_callable = func_carrega_dados_clientes,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_CLIENTE.csv'}},
        dag = dag_lab6_dsa
    )


##### Tabela de Transportadoras #####

def func_carrega_dados_transp(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_transp = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_transp = "INSERT INTO lab6.DIM_TRANSPORTADORA (%s) VALUES (%s)" % (','.join(dados_transp.keys()), ','.join([item for item in dados_transp.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_transp_' + str(i), 
                                                 sql = sql_query_transp, 
                                                 params = (dados_transp), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_transportadora = PythonOperator(
        task_id = 'tarefa_carrega_dados_transportadora',
        python_callable = func_carrega_dados_transp,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_TRANSPORTADORA.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Depósitos #####

def func_carrega_dados_dep(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_dep = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_dep = "INSERT INTO lab6.DIM_DEPOSITO (%s) VALUES (%s)" % (','.join(dados_dep.keys()), ','.join([item for item in dados_dep.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_dep_' + str(i), 
                                                 sql = sql_query_dep, 
                                                 params = (dados_dep), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_deposito = PythonOperator(
        task_id = 'tarefa_carrega_dados_deposito',
        python_callable = func_carrega_dados_dep,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_DEPOSITO.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Entregas #####

def func_carrega_dados_ent(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_ent = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_ent = "INSERT INTO lab6.DIM_ENTREGA (%s) VALUES (%s)" % (','.join(dados_ent.keys()), ','.join([item for item in dados_ent.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_ent_' + str(i), 
                                                 sql = sql_query_ent, 
                                                 params = (dados_ent), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_entrega = PythonOperator(
        task_id = 'tarefa_carrega_dados_entrega',
        python_callable = func_carrega_dados_ent,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_ENTREGA.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Frete #####

def func_carrega_dados_frete(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_frete = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_frete = "INSERT INTO lab6.DIM_FRETE (%s) VALUES (%s)" % (','.join(dados_frete.keys()), ','.join([item for item in dados_frete.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_frete_' + str(i), 
                                                 sql = sql_query_frete, 
                                                 params = (dados_frete), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_frete = PythonOperator(
        task_id = 'tarefa_carrega_dados_frete',
        python_callable = func_carrega_dados_frete,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_FRETE.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Tipos de Pagamentos #####

def func_carrega_dados_pagamento(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_pag = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_pag = "INSERT INTO lab6.DIM_PAGAMENTO (%s) VALUES (%s)" % (','.join(dados_pag.keys()), ','.join([item for item in dados_pag.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_pag_' + str(i), 
                                                 sql = sql_query_pag, 
                                                 params = (dados_pag), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_pagamento = PythonOperator(
        task_id = 'tarefa_carrega_dados_pagamento',
        python_callable = func_carrega_dados_pagamento,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_PAGAMENTO.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Data #####

def func_carrega_dados_data(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_data = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_data = "INSERT INTO lab6.DIM_DATA (%s) VALUES (%s)" % (','.join(dados_data.keys()), ','.join([item for item in dados_data.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_data_' + str(i), 
                                                 sql = sql_query_data, 
                                                 params = (dados_data), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_data = PythonOperator(
        task_id = 'tarefa_carrega_dados_data',
        python_callable = func_carrega_dados_data,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/DIM_DATA.csv'}},
        dag = dag_lab6_dsa
    )

##### Tabela de Fatos #####

def func_carrega_dados_fatos(**kwargs):
    
    # Get the csv file path
    csv_file_path = kwargs['params']['csv_file_path']

    # Inicializa o contador
    i = 0

    # Open the csv file
    with open(csv_file_path, 'r') as f:

        reader = csv.DictReader(f)

        for item in reader:

            # Icrementa o contador
            i += 1
            
            # Extrai uma linha como dicionário
            dados_fatos = dict(item)

            # Insert data into the PostgreSQL table
            sql_query_fatos = "INSERT INTO lab6.TB_FATO (%s) VALUES (%s)" % (','.join(dados_fatos.keys()), ','.join([item for item in dados_fatos.values()]))
    
            # Operador do Postgres com incremento no id da tarefa (para cada linha inserida)
            postgres_operator = PostgresOperator(task_id = 'carrega_dados_fatos_' + str(i), 
                                                 sql = sql_query_fatos, 
                                                 params = (dados_fatos), 
                                                 postgres_conn_id = 'Lab6DW', 
                                                 dag = dag_lab6_dsa)
    
            # Executa o operador
            postgres_operator.execute(context = kwargs)


tarefa_carrega_dados_fatos = PythonOperator(
        task_id = 'tarefa_carrega_dados_fatos',
        python_callable = func_carrega_dados_fatos,
        provide_context = True,
        op_kwargs = {'params': {'csv_file_path': '/opt/airflow/dags/dados/TB_FATO.csv'}},
        dag = dag_lab6_dsa
    )


# Tarefas para limpar as tabelas
tarefa_trunca_tb_fato = PostgresOperator(task_id = 'tarefa_trunca_tb_fato', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.TB_FATO CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_cliente = PostgresOperator(task_id = 'tarefa_trunca_dim_cliente', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_CLIENTE CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_pagamento = PostgresOperator(task_id = 'tarefa_trunca_dim_pagamento', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_PAGAMENTO CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_frete = PostgresOperator(task_id = 'tarefa_trunca_dim_frete', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_FRETE CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_data = PostgresOperator(task_id = 'tarefa_trunca_dim_data', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_DATA CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_transportadora = PostgresOperator(task_id = 'tarefa_trunca_dim_transportadora', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_TRANSPORTADORA CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_entrega = PostgresOperator(task_id = 'tarefa_trunca_dim_entrega', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_ENTREGA CASCADE", dag = dag_lab6_dsa)
tarefa_trunca_dim_deposito = PostgresOperator(task_id = 'tarefa_trunca_dim_deposito', postgres_conn_id = 'Lab6DW', sql = "TRUNCATE TABLE lab6.DIM_DEPOSITO CASCADE", dag = dag_lab6_dsa)

# Upstream
tarefa_trunca_tb_fato >> tarefa_trunca_dim_cliente >> tarefa_trunca_dim_pagamento >> tarefa_trunca_dim_frete >> tarefa_trunca_dim_data >> tarefa_trunca_dim_transportadora >> tarefa_trunca_dim_entrega >> tarefa_trunca_dim_deposito >> tarefa_carrega_dados_clientes >> tarefa_carrega_dados_transportadora >> tarefa_carrega_dados_deposito >> tarefa_carrega_dados_entrega >> tarefa_carrega_dados_frete >> tarefa_carrega_dados_pagamento >> tarefa_carrega_dados_data >> tarefa_carrega_dados_fatos

# Bloco main
if __name__ == "__main__":
    dag_lab6_dsa.cli()



