import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': "Michel",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 11)
}

@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['Titanic', 'Final', 'DAG2'])
def trabalho_dag_02():

    @task
    def tabela_final():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def tabela_resultados(NOME_DO_ARQUIVO):
        TABELA_RESULTADOS = "/tmp/resultados.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Pclass']).agg({"PassengerId":"mean", "Fare":"mean", "parentes":"mean"}).reset_index()
        print(res)
        res.to_csv(TABELA_RESULTADOS, index=False, sep=";")
        return TABELA_RESULTADOS

    start = DummyOperator(task_id="inicio")
    end = DummyOperator(task_id="fim")

    unica = tabela_final()
    resultados = tabela_resultados(unica)

    start >> unica >> resultados >> end

execucao = trabalho_dag_02()