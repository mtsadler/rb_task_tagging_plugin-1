import pytest
from airflow.configuration import conf
import logging


@pytest.mark.compatibility
@pytest.mark.parametrize(
    ("curr_section", "curr_key", "expected_value"),
    [
        ("core", "enable_xcom_pickling", "False"),
    ],
)
def test_check_configuration_value(curr_section, curr_key, expected_value):
    """
    Checks that airflow.cfg's configurations are set properly for this plugin.
    """
    config_val = conf.get(section=curr_section, key=curr_key)
    logging.info(f"{ curr_section }.{ curr_key } is set to { config_val }")
    assert config_val == expected_value
