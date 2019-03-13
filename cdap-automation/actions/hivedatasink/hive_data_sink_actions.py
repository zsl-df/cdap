from nimble.core import global_constants
from nimble.core.utils.shell_utils import ShellUtils

from actions.base_accelerator_actions import BaseAcceleratorActions


class HiveDataSinkActions(BaseAcceleratorActions):
    def __init__(self, job_alias):
        super(HiveDataSinkActions, self).__init__()
        self.tmp_input_path = "%s/tmp_input/" % global_constants.DEFAULT_LOCAL_TMP_PATH
        self.file_server_input_path = "modules/accelerators/cdap/hive_data_sink/input/"
        self.job_alias = job_alias
        ShellUtils.execute_shell_command(ShellUtils.remove_and_create_directory(self.tmp_input_path))

    def validate_hive_data_sink(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select * from %(job)s_input1""" % {"job": self.job_alias})
