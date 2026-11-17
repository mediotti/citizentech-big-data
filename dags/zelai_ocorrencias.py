"""Orquestra o snapshot analítico e a publicação do painel."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task
from zelai_data import pipeline


@dag(
    dag_id="zelai_ocorrencias",
    description="Ocorrências ZelaAí: ingestão, tratamento, carga, qualidade e painel",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="0 9 * * *",  # 06h em São Paulo.
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["citizentech", "zelai", "etl"],
)
def daily_snapshot():
    @task
    def ingerir():
        context = get_current_context()
        pipeline.extract(context["run_id"])

    @task
    def tratar():
        pipeline.transform(get_current_context()["run_id"])

    @task
    def carregar():
        pipeline.load(get_current_context()["run_id"])

    @task
    def validar():
        pipeline.validate(get_current_context()["run_id"])

    @task
    def publicar():
        pipeline.publish(get_current_context()["run_id"])

    ingerir() >> tratar() >> carregar() >> validar() >> publicar()


dag = daily_snapshot()
