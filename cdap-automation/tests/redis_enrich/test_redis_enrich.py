import time

import pytest
from nimble.core import global_constants
from nimble.core.entity.node_manager import NodeManager
from nimble.core.utils.components.cdap_utils import CdapUtils
from nimble.core.utils.file_server_utils import FileServerUtils

from actions.base_accelerator_actions import BaseAcceleratorActions
from nimble.core.entity.components import Components
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils
from actions.redis_enrich.redis_enrich_actions import RedisEnrichActions
from nimble.core.utils.components.redis_utils import RedisUtils, DataType

file_server_input_data_path = "modules/accelerators/cdap/redisEnrich/input/"


class TestRedisEnrich(object):
    job_alias = "redis_enrich"
    guavuspluginversion = "5.3.0-2.0.0"

    @pytest.fixture(scope="session")
    def redis_enrich_actions(self):
        return RedisEnrichActions(self.job_alias)

    @pytest.fixture(scope="session")
    def redis_utils(self):
        return RedisUtils()

    @pytest.fixture(scope="session")
    def file_server_utils(self):
        return FileServerUtils()

    def test_import_file(self, redis_utils, file_server_utils):

        file_server_utils.download("modules/accelerators/cdap/redisEnrich/redisinput/redisinput.txt")
        file_path = "%s/redisinput.txt" % global_constants.DEFAULT_LOCAL_TMP_PATH
        assert redis_utils.import_file(file_path, DataType.STRING.value, delimiter=",")

    @pytest.mark.skip("Can't be automable")
    def test_validate_RedisEnrich_plugin_seen_on_cdap_after_running_ansible(self, user_actions, elastic_search_actions,
                                                                            payload):
        """
        Functional Testcases to be covered:-
        1.) Validate that RedisEnrich plugin should be in the analytics category after running the ansible script.

        """

    @pytest.mark.skip("Can't be automable")
    def test_validate_plugin_cannot_be_deployed_if_mandatory_field_is_empty(self, user_actions, elastic_search_actions,
                                                                            payload):
        """
        Functional Testcases to be covered:-

        1.) Validate that Plugin can't be depolyed if any of of the mandatory field is empty.

        """

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/redisEnrich/pipelines/rediswithkeyid-cdap-data-pipeline.json",
        file_name_regex="*withkeyid*"))
    def test_validate_data_when_key_present_redis(self, user_actions, redis_enrich_actions,
                                                  payload):

        """
        Functional Testcases to be covered:-
                    1.) Validate the data when key is present in the Redis
                    2.) Validate the data when key is not present in the Redis.
        """
        pipeline_name = "automation_redis_enrich_id_%s" % NodeManager.current_time
        dataset_name = "automation_redis_enrich_dataset_id_%s" % NodeManager.current_time
        Redislist = NodeManager.node_obj.get_node_ips_by_component(Components.REDIS.name)
        redis_servers = ",".join("%s:6379" % i for i in Redislist)
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/redisenrich/")
        DynamicSubstitutionUtils.add({"randynamic_redis_servers": redis_servers})
        redis_enrich_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset_name,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)

        user_actions.validate(redis_enrich_actions.validate_redis_enrich, self.job_alias,
                              dataset_alias="enrichid", value="str1")
        redis_enrich_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/redisEnrich/pipelines/rediswithkeyintid-cdap-data-pipeline.json",
        file_name_regex="*intid*"))
    def test_validate_key_is_integer(self, user_actions, redis_enrich_actions, payload):

        """
        Functional Testcases to be covered:-

        1.) Validate that the plugin when key is of integer.

        """

        pipeline_name = "automation_redis_enrich_intid_%s" % NodeManager.current_time
        dataset_name = "automation_redis_enrich_dataset_intid_%s" % NodeManager.current_time
        Redislist = NodeManager.node_obj.get_node_ips_by_component(Components.REDIS.name)
        redis_servers = ",".join("%s:6379" % i for i in Redislist)
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/redisenrich/")
        DynamicSubstitutionUtils.add({"randynamic_redis_servers": redis_servers})
        redis_enrich_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset_name,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)

        user_actions.validate(redis_enrich_actions.validate_redis_enrich, self.job_alias,
                              dataset_alias="enrichintid", value="age_student")
        redis_enrich_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/redisEnrich/pipelines/rediswithkeyfloatid-cdap-data-pipeline.json",
        file_name_regex="*floatid*"))
    def test_validate_key_is_float(self, user_actions, redis_enrich_actions, payload):

        """
        Functional Testcases to be covered:-

        1.) Validate that the plugin when key is of float.

        """

        pipeline_name = "automation_redis_enrich_floatid_%s" % NodeManager.current_time
        dataset_name = "automation_redis_enrich_dataset_floatid_%s" % NodeManager.current_time
        Redislist = NodeManager.node_obj.get_node_ips_by_component(Components.REDIS.name)
        redis_servers = ",".join("%s:6379" % i for i in Redislist)
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/redisenrich/")
        DynamicSubstitutionUtils.add({"randynamic_redis_servers": redis_servers})
        redis_enrich_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset_name,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)

        user_actions.validate(redis_enrich_actions.validate_redis_enrich, self.job_alias,
                              dataset_alias="enrichfloatid", value="marks")
        redis_enrich_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/redisEnrich/pipelines/rediswithkeyoutputschema-cdap-data-pipeline.json",
        file_name_regex="*outputschema*"))
    def test_validate_redis_value_is_string_and_in_output_schema_is_integer(self, user_actions, redis_enrich_actions,
                                                                            payload):
        """
        Functional Testcases to be covered:-

        1.) Validate that if the value from the redis is string type but in output schema we change to another data type.

        """
        pipeline_name = "automation_redis_outputschema_%s" % NodeManager.current_time
        dataset_name = "automation_redis_outputschema_%s" % NodeManager.current_time
        Redislist = NodeManager.node_obj.get_node_ips_by_component(Components.REDIS.name)
        redis_servers = ",".join("%s:6379" % i for i in Redislist)
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/redisenrich/")
        DynamicSubstitutionUtils.add({"randynamic_redis_servers": redis_servers})
        redis_enrich_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset_name,
                                                   guavuspluginversion=self.guavuspluginversion)

        flag = True
        try:
            user_actions.schedule(self.job_alias)
            flag = False
        except AssertionError:
            assert 1
        if flag is False:
            assert 0, "CDAP pipeline passed when it shouldn't"
