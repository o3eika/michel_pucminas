import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Michel",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 11)
}

@dag(default_args=default_args, schedule_interval='*/1 * * * *', catchup=False, tags=['Titanic', 'Final', 'DAG1'])
def trabalho_dag_01():

    @task
    def obtendo_dados():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=";")
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO
    
    @task
    def ind_passageiros(NOME_DO_ARQUIVO):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({"PassengerId": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA
    
    @task
    def ind_preco(NOME_DO_ARQUIVO):
        NOME_TABELA2 = "/tmp/preco_por_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    @task
    def ind_sibsp_parch(NOME_DO_ARQUIVO):
        NOME_TABELA3 = "/tmp/parentes_por_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df['parentes'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg({"parentes": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    @task
    def tabela_final(NOME_TABELA, NOME_TABELA2, NOME_TABELA3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df1 = pd.read_csv(NOME_TABELA, sep=";")
        df2 = pd.read_csv(NOME_TABELA2, sep=";")
        df3 = pd.read_csv(NOME_TABELA3, sep=";")

        df4 = df1.merge(df2, on=['Sex','Pclass'], how='inner')
        df5 = df4.merge(df3, on=['Sex','Pclass'], how='inner')
        print(df5)
        df5.to_csv(TABELA_UNICA, index=False, sep=";")
        return TABELA_UNICA    

    start = DummyOperator(task_id = "inicio")
    fim = DummyOperator(task_id = "fim")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def end():
        print("Terminou")
    triggerdag = TriggerDagRunOperator(
        task_id = "trigga_dag_02",
        trigger_dag_id = "trabalho_dag_02"
        )

    obt = obtendo_dados()
    ind = ind_passageiros(obt)
    indp = ind_preco(obt)
    indpar = ind_sibsp_parch(obt)
    tabfin = tabela_final(ind, indp, indpar)

    start >> obt >> [ind, indp, indpar] >> tabfin >> fim >> triggerdag

execucao = trabalho_dag_01()