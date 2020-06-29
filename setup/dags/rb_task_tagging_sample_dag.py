from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from rb_task_tagging_plugin.core.helpers.task_tagging_helpers import (
    set_xcom_tags,
    get_many_xcom_tags,
)
from airflow.utils import timezone
from datetime import datetime, timedelta
import json

DAG_ID = "example_custom_xcom_use"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jgormley@raybeam.com"],
    "start_date": datetime(2020, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}


# Define methods for pushing key/value pairs to XCOM
def send_value_via_xcom_1(**context):
    print("xcom push task 1")

    set_xcom_tags(
        context,
        key="bbb_tags",
        value=json.loads(
            '{ "activity":"test", "level":"bo", "classification":"quality" }'
        ),
    )


def send_value_via_xcom_2(**context):
    print("xcom push task 2")

    set_xcom_tags(
        context,
        key="bbb_tags",
        value=json.loads('{ "activity":"transfer", "level":"source" }'),
    )


def send_value_via_xcom_3(**context):
    print("xcom push task 3")

    set_xcom_tags(
        context,
        key="bbb_tags",
        value=json.loads('{ "activity":"transform", "level":"raw" }'),
    )


# Define a method for pulling a key/value pair via custom method
def get_value_via_custom_xcom(**context):
    print("XCom custom get_many testing")

    returned_xcom_tasks = get_many_xcom_tags(
        min_execution_date=timezone.datetime(2020, 6, 18, 19, 58, 39, 00),
        max_execution_date=context["ti"].execution_date,
        dag_ids="example_custom_xcom_use",
        key="bbb_tags",
        values=json.loads('{ "activity":"transfer", "level":"source" }'),
    )

    print("The following tasks were returned")
    print(returned_xcom_tasks)

    for match in returned_xcom_tasks:
        print("Task match: ", match.task_id)
        # print('At this time: ', match.execution_date)
        print("For this value: ", match.value)
        # value_json = json.loads(match.value)
        # print('For this value: ', value_json["activity"])


dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    schedule_interval=None,
    max_active_runs=1,
)

start = DummyOperator(task_id="Start", dag=dag)

end = DummyOperator(task_id="End", dag=dag)

xcom_operator_push_1 = PythonOperator(
    task_id="xcom_push_task_1",
    python_callable=send_value_via_xcom_1,
    provide_context=True,
    dag=dag,
)

xcom_operator_push_2 = PythonOperator(
    task_id="xcom_push_task_2",
    python_callable=send_value_via_xcom_2,
    provide_context=True,
    retries=1,
    dag=dag,
)

xcom_operator_push_3 = PythonOperator(
    task_id="xcom_push_task_3",
    python_callable=send_value_via_xcom_3,
    provide_context=True,
    dag=dag,
)

xcom_operator_custom_pull = PythonOperator(
    task_id="xcom_testing_custom_pull",
    python_callable=get_value_via_custom_xcom,
    provide_context=True,
    dag=dag,
)

start >> xcom_operator_push_1 >> xcom_operator_push_2 >> xcom_operator_push_3 >> xcom_operator_custom_pull >> end
