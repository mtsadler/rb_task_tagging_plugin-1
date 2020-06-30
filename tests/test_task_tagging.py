from core.helpers.task_tagging_helpers import (
    set_xcom_tags,
    get_many_xcom_tags,
)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from airflow.models.xcom import XCom
from datetime import datetime
import unittest
import json
from airflow.configuration import conf

DEFAULT_DATE = "2020-06-01"
TEST_DAG_ID = "test_task_tagging"
TEST_KEY = "bbb_tags_testing"
TEST_VALUE = '{ "tag1":"val1", "tag2":"val2", "tag3":"val3" }'
TEST_VALUE_DICT = json.loads(TEST_VALUE)
TEST_VALUE_FORMATTED_JSON = json.dumps(TEST_VALUE_DICT)
TEST_VALUE_SERIALIZED = XCom.serialize_value(TEST_VALUE_FORMATTED_JSON)


class TaskTaggingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag = DAG(
            TEST_DAG_ID,
            schedule_interval=None,
            default_args={"start_date": DEFAULT_DATE},
        )
        cls.op = DummyOperator(task_id="task_for_testing_tagging", dag=cls.dag)
        cls.ti = TaskInstance(task=cls.op, execution_date=datetime(2020, 6, 1))

    @provide_session
    def tearDown(self, session):
        session.query(XCom).filter(
            XCom.key == TEST_KEY,
            XCom.task_id == self.ti.task_id,
            XCom.dag_id == self.ti.dag_id,
        ).delete()

    # test setting tags via wrapper function
    def test_setting_tags(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY, TEST_VALUE_DICT)

        test_set_results = self.ti.xcom_pull(
            key=TEST_KEY, task_ids=self.ti.task_id, dag_id=self.ti.dag_id
        )

        self.assertEqual(test_set_results, TEST_VALUE_FORMATTED_JSON)

    # test pulling tags that do not exist
    @provide_session
    def test_pull_nonexistent_tags(self, session):

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            task_ids=self.ti.task_id,
            key=TEST_KEY,
            values=TEST_VALUE_DICT,
        )

        # assert list returned is empty, no xcom matched
        self.assertFalse(returned_xcom_tasks)

    # test pulling tags that do exist
    def test_pull_existing_tags(self):

        # self.ti.xcom_push(TEST_KEY, TEST_VALUE_FORMATTED_JSON)

        if conf.getboolean("core", "enable_xcom_pickling"):
            print("Pickling is enabled")
        else:
            print('Picking is disabled')

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY, TEST_VALUE_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            task_ids=self.ti.task_id,
            key=TEST_KEY,
            # values=TEST_VALUE_DICT,
        )

        print("Returned xcoms: ", returned_xcom_tasks)
        if returned_xcom_tasks:
            print("Returned value: ", returned_xcom_tasks[0].value)
            print(
                "Returned value: ", type(returned_xcom_tasks[0].value)
            )

        # assert list returned is not empty, xcom matches were found
        self.assertTrue(bool(returned_xcom_tasks))

        returned_xcom_tasks_w_value = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            task_ids=self.ti.task_id,
            key=TEST_KEY,
            values=TEST_VALUE_DICT,
        )

        print("Returned xcoms: ", returned_xcom_tasks_w_value)
        if returned_xcom_tasks_w_value:
            print("Returned value: ", returned_xcom_tasks_w_value[0].value)
            print(
                "Returned value: ", type(returned_xcom_tasks_w_value[0].value)
            )

        self.assertTrue(bool(returned_xcom_tasks_w_value))


if __name__ == "__main__":
    unittest.main()
