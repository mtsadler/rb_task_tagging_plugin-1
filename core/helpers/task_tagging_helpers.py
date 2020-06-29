from airflow.models.xcom import XCom
from airflow.utils.db import provide_session
from airflow.utils.helpers import as_tuple
from sqlalchemy import and_
import json


def set_xcom_tags(context, key, value):
    """Wrapper to set a task's XCom tags.

    Sets a task's XComs tags by calling the task instance xcom_push method. The
    tag value is set as a JSON formatted string.

    Args:
        key (string): The tag's key.
        value (dict): The tag's values, a JSON object.
    """

    context["ti"].xcom_push(
        key=key, value=json.dumps(value),
    )


@provide_session
def get_many_xcom_tags(
    min_execution_date=None,
    max_execution_date=None,
    key=None,
    values=None,
    task_ids=None,
    dag_ids=None,
    limit=100,
    session=None,
):
    """Retrieve XComs, optionally meeting certain criteria.

    XComs are queried based on execution date, key, value, task_ids, and
        dag_ids. The value is passed in as a dict and then serialized to a JSON
        formatted string to be split up into a list containing the individual
        key/value pairs.

    Args:
        min_execution_date (datetime): The min date to filter on.
        max_execution_date (datetime): The max date to filter on.
        key (string): The key to filter on.
        values (dict): A JSON object of values to filter on.
        task_ids (str or iterable of strings (representing task_ids)): The
            tag_ids to filter on.
        dag_ids (str or iterable of strings (representing dag_ids)): The
            dag_ids to filter on.
        limit (int): The max number of results to be returned by XComs query.

    Returns:
        A list of XCom objects that matched the filter criteria. If no matches
            were found, then None is returned.
    """

    filters = []
    cleaned_values = []

    # split the values into a list of individual key/value pairs
    if values:
        values = json.dumps(values)[1:-1].strip().split(",")

        for item in values:
            cleaned_values.append(
                XCom.serialize_value("%" + item.strip() + "%")
            )

    if key:
        filters.append(XCom.key == key)
    if values:
        for filter_value in cleaned_values:
            filters.append(XCom.value.like(filter_value))
    if task_ids:
        filters.append(XCom.task_id.in_(as_tuple(task_ids)))
    if dag_ids:
        filters.append(XCom.dag_id.in_(as_tuple(dag_ids)))
    if min_execution_date:
        filters.append(XCom.execution_date >= min_execution_date)
    if max_execution_date:
        filters.append(XCom.execution_date <= max_execution_date)

    query = (
        session.query(XCom)
        .filter(and_(*filters))
        .order_by(XCom.execution_date.desc(), XCom.timestamp.desc())
        .limit(limit)
    )
    results = query.all()
    return results
