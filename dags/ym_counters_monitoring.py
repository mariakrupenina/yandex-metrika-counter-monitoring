from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


"""
Airflow Variables, которые нужно создать:

YM_OAUTH_TOKEN

YM_COUNTER_1
    ....
YM_COUNTER_n

SMTP_LOGIN
SMTP_PASSWORD
EMAIL_TO
"""


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# если metrika_monitor.py лежит в репозитории в папке src/
# а DAG лежит в папке dags/
REPO_ROOT = os.path.dirname(CURRENT_DIR)
SRC_DIR = os.path.join(REPO_ROOT, "src")

if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from metrika_monitor import main


def run_metrika_monitor():
    os.environ["YM_OAUTH_TOKEN"] = Variable.get("YM_OAUTH_TOKEN")

    os.environ["YM_COUNTER_SBL"] = Variable.get("YM_COUNTER_SBL")
    os.environ["YM_COUNTER_PRO"] = Variable.get("YM_COUNTER_PRO")
    os.environ["YM_COUNTER_CIB"] = Variable.get("YM_COUNTER_CIB")
    os.environ["YM_COUNTER_INDIA"] = Variable.get("YM_COUNTER_INDIA")
    os.environ["YM_COUNTER_LEGAL"] = Variable.get("YM_COUNTER_LEGAL")

    os.environ["SMTP_LOGIN"] = Variable.get("SMTP_LOGIN")
    os.environ["SMTP_PASSWORD"] = Variable.get("SMTP_PASSWORD")
    os.environ["EMAIL_TO"] = Variable.get("EMAIL_TO")

    main()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ym_counters_monitoring",
    default_args=default_args,
    description="Мониторинг поступления данных по счетчикам Яндекс Метрики",
    start_date=datetime(2026, 3, 24),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["yandex_metrika", "monitoring", "alerts"],
) as dag:

    run_monitor = PythonOperator(
        task_id="run_metrika_monitor",
        python_callable=run_metrika_monitor,
    )

    run_monitor
