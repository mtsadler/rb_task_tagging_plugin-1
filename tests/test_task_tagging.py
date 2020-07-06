from rb_task_tagging_plugin.core.helpers.task_tagging_helpers import (
    set_xcom_tags,
    get_many_xcom_tags,
)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from airflow.models.xcom import XCom
from airflow.utils import timezone
from datetime import datetime
import unittest
import json
import pytest

DEFAULT_DATE = "2020-06-01"
TEST_DAG_ID = "test_task_tagging"
TEST_KEY_ONE = "bbb_tags_testing_one"
TEST_KEY_TWO = "bbb_tags_testing_two"
TEST_VALUE_ALL = '{ "tag1":"val1", "tag2":"val2", "tag3":"val3" }'
TEST_VALUE_1 = '{ "tag1":"val1" }'
TEST_VALUE_2 = '{ "tag2":"val2" }'
TEST_VALUE_3 = '{ "tag3":"val3" }'
TEST_VALUE_ALL_DICT = json.loads(TEST_VALUE_ALL)
TEST_VALUE_1_DICT = json.loads(TEST_VALUE_1)
TEST_VALUE_2_DICT = json.loads(TEST_VALUE_2)
TEST_VALUE_3_DICT = json.loads(TEST_VALUE_3)


@pytest.mark.compatibility
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
            XCom.dag_id == self.ti.dag_id,
        ).delete()

    # test setting tags via wrapper function
    def test_setting_tags(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        test_set_results = self.ti.xcom_pull(
            key=TEST_KEY_ONE, task_ids=self.ti.task_id, dag_id=self.ti.dag_id
        )

        self.assertEqual(test_set_results, TEST_VALUE_ALL_DICT)

    # test pulling tags that do not exist
    def test_pull_nonexistent_tags(self):

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 0, no xcom matched
        self.assertEqual(len(returned_xcom_tasks), 0)

    # test pulling tags that do exist, only one, by dag_id
    def test_pull_existing_tags_dag_id(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 1, xcom match was found
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, only one, by task_id
    def test_pull_existing_tags_task_id(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            task_ids=self.ti.task_id,
        )

        # assert list returned is of length 1, xcom match was found
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, only one, by key
    def test_pull_existing_tags_key(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            key=TEST_KEY_ONE
        )

        # assert list returned is of length 1, xcom match was found
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, only one, all filter values
    def test_pull_existing_tags_all_values(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            values=TEST_VALUE_ALL_DICT,
        )

        # assert list returned is of length 1, xcom match was found
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, only one, one filter value at a time
    def test_pull_existing_tags_each_value(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            values=TEST_VALUE_1_DICT,
        )

        # assert list returned is of length 1, xcom match was found for value 1
        self.assertEqual(len(returned_xcom_tasks), 1)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            values=TEST_VALUE_2_DICT,
        )

        # assert list returned is of length 1, xcom match was found for value 2
        self.assertEqual(len(returned_xcom_tasks), 1)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            values=TEST_VALUE_3_DICT,
        )

        # assert list returned is of length 1, xcom match was found for value 3
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, only one, multiple exec date filters
    def test_pull_existing_tags_exec_dates(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            min_execution_date=self.ti.execution_date,
            max_execution_date=self.ti.execution_date,
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 1, xcom match was found for date =
        self.assertEqual(len(returned_xcom_tasks), 1)

        returned_xcom_tasks = get_many_xcom_tags(
            min_execution_date=timezone.datetime(2020, 5, 31, 19, 58, 39, 00),
            max_execution_date=timezone.datetime(2050, 6, 18, 19, 58, 39, 00),
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 1, xcom match was found for range
        self.assertEqual(len(returned_xcom_tasks), 1)

        returned_xcom_tasks = get_many_xcom_tags(
            min_execution_date=timezone.datetime(2050, 6, 18, 19, 58, 39, 00),
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 0, xcom match was not found for min
        self.assertEqual(len(returned_xcom_tasks), 0)

        returned_xcom_tasks = get_many_xcom_tags(
            max_execution_date=timezone.datetime(2020, 5, 31, 19, 58, 39, 00),
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is of length 0, xcom match was not found for max
        self.assertEqual(len(returned_xcom_tasks), 0)

    # test pulling tags that do exist, using limit arg
    def test_pull_existing_tags_limit(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)
        set_xcom_tags(context, TEST_KEY_TWO, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
            limit=1
        )

        # assert list returned is not empty, xcom matches were found
        self.assertEqual(len(returned_xcom_tasks), 1)

    # test pulling tags that do exist, more than one
    def test_pull_existing_tags_multiple(self):

        context = self.ti.get_template_context()

        set_xcom_tags(context, TEST_KEY_ONE, TEST_VALUE_ALL_DICT)
        set_xcom_tags(context, TEST_KEY_TWO, TEST_VALUE_ALL_DICT)

        returned_xcom_tasks = get_many_xcom_tags(
            dag_ids=self.ti.dag_id,
        )

        # assert list returned is not empty, xcom matches were found
        self.assertEqual(len(returned_xcom_tasks), 2)


if __name__ == "__main__":
    unittest.main()
