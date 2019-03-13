import time

import pytest
from nimble.core.entity.node_manager import NodeManager
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils

from actions.base_accelerator_actions import BaseAcceleratorActions
from actions.hivedatasink.hive_data_sink_actions import HiveDataSinkActions

file_server_payload_path = "modules/accelerators/cdap/hive_data_sink/pipelines/"
file_server_input_data_path = "modules/accelerators/cdap/hive_data_sink/input/"


class TestHiveDataSink(object):
    job_alias = "hive_data_sink"

    @pytest.fixture(scope="session")
    def hive_data_sink_actions(self):
        return HiveDataSinkActions(self.job_alias)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(file_server_payload_path))
    def test_validate_hive_data_sink(self, user_actions, hive_data_sink_actions, payload, capsys):
        pipeline_name = "automation_hive_data_sink"
        db_name = "automation_hive"
        table_name = "automation_hive_table_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/")

        DynamicSubstitutionUtils.add({"randynamic_db_name": db_name, "randynamic_table_name": table_name})
        hive_data_sink_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(120)
        user_actions.validate(hive_data_sink_actions.validate_hive_data_sink, self.job_alias, capsys)
