import logging.config
import os

import pytest

from nimble.actions.base.flows.user_actions import UserActions
from nimble.core import global_constants
from nimble.core.configs.validation_config_parser import ValidationConfigParser
from nimble.core.entity.node_manager import NodeManager
from nimble.core.utils.report_utils import ReportUtils

try:
    os.makedirs(global_constants.DEFAULT_LOCAL_ARTIFACTS_PATH)
except Exception:
    pass

logging.config.fileConfig(global_constants.DEFAULT_LOGGING_FILE_PATH)

OPTIONS_DICT = {}
PREVIOUS_FAILED = None
ITEM_LIST = []


def pytest_addoption(parser):
    parser.addoption("--testbed",
                     help="Relative path (to the project root) of the testbed file. E.g. python -m pytest --testbed=resources/modules/mrx/testbeds/mrx_qa_testbed.cfg")

    parser.addoption("--validationConfig",
                     help="Relative path (to the project root) of the file containing validation configs. E.g. python -m pytest --validationConfig=resources/modules/platform/validation/sample_validation_config.yml")


@pytest.fixture(scope="session", autouse=True)
def initialize_node_obj(request):
    testbed_file = request.config.getoption("--testbed")
    if testbed_file:
        NodeManager.initialize(testbed_file)


@pytest.fixture(scope="session", autouse=True)
def initialize_arguments(request):
    global OPTIONS_DICT

    for option, value in request.config.option.__dict__.items():
        OPTIONS_DICT[option] = value


@pytest.fixture(scope="session")
def config_parser(initialize_arguments):
    """Initialize the validation config parser.

    :return: Return the object of the Validation config parser.
    :rtype: :class:`core.configs.validation_config_parser.ValidationConfigParser`
    """
    return ValidationConfigParser(OPTIONS_DICT["validationConfig"])


@pytest.fixture(scope="session", autouse=True)
def dump_allure_env_file(config_parser):
    """Dump the basic environment variables for Allure.

    :param config_parser: Fixture defined above.
    """
    report_dict = ReportUtils.get_generic_attributes(config_parser)
    ReportUtils.dump_allure_env_file(report_dict)


@pytest.fixture(scope="session")
def user_actions(config_parser):
    """Initialize the object for user actions.

    :param config_parser: Fixture defined above.
    :rtype: :class:`actions.base.flows.user_actions.UserActions`
    """
    return UserActions(config_parser)
