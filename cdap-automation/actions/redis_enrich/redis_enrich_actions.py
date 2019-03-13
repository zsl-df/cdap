from nimble.core import global_constants
from nimble.core.utils.shell_utils import ShellUtils

from actions.base_accelerator_actions import BaseAcceleratorActions


class RedisEnrichActions(BaseAcceleratorActions):
    def __init__(self, job_alias):
        super(RedisEnrichActions, self).__init__()
        self.tmp_input_path = "%s/tmp_input/" % global_constants.DEFAULT_LOCAL_TMP_PATH
        self.file_server_input_path = "modules/accelerators/cdap/elasticsearch/input/"
        self.job_alias = job_alias
        ShellUtils.execute_shell_command(ShellUtils.remove_and_create_directory(self.tmp_input_path))

    def validate_redis_enrich(self, validation_entities,value):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select ifnull("str1","") as "str1",
                  ifnull("age_student","") as "age_student",
                  ifnull("marks","") as "marks",
                  ifnull("address","") as address,
                  ifnull("c_2","") as lastname from %(job)s_input1 left join %(job)s_input2 on
                    %(job)s_input1.%(value)s = %(job)s_input2.c_1""" % {"job":self.job_alias,"value":value})
