# Lab 5 - Job ETL

# Imports
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# Argumentos
args = {'owner': 'airflow'}

# Argumentos default
default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

# Cria a DAG
dag_lab5_dsa = DAG(dag_id = "dag_lab5_dsa",
                   default_args = args,
                   # schedule_interval='0 0 * * *',
                   schedule_interval = '@once',  
                   dagrun_timeout = timedelta(minutes = 60),
                   description = 'Job ETL de Carga no DW com Airflow',
                   start_date = airflow.utils.dates.days_ago(1)
)

# Instrução SQL de criação de tabela
sql_cria_tabela = """CREATE TABLE IF NOT EXISTS dwdb.tb_funcionarios (id INT NOT NULL, nome VARCHAR(250) NOT NULL, departamento VARCHAR(250) NOT NULL);"""

# Tarefa de criação da tabela
cria_tabela = PostgresOperator(sql = sql_cria_tabela,
                               task_id = "tarefa_cria_tabela",
                               postgres_conn_id = "Postgres",
                               dag = dag_lab5_dsa
)

# Instrução SQL de insert na tabela
sql_insere_dados = """
insert into dwdb.tb_funcionarios (id, nome, departamento) values (1000, 'Bob', 'Marketing'), (1001, 'Maria', 'Contabilidade'),(1002, 'Jeremias', 'Engenharia de Dados'), (1003, 'Messi', 'Marketing') ;"""

# Tarefa de insert na tabela
insere_dados = PostgresOperator(sql = sql_insere_dados,
                                task_id = "tarefa_insere_dados",
                                postgres_conn_id = "Postgres",
                                dag = dag_lab5_dsa
)

# Fluxo da DAG
cria_tabela >> insere_dados

# Bloco main
if __name__ == "__main__":
    dag_lab5_dsa.cli()



