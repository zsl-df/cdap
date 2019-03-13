import time

import pytest
from nimble.core.entity.node_manager import NodeManager

from actions.base_accelerator_actions import BaseAcceleratorActions
from actions.joinplugin.join_plugin_sink_actions import JoinPluginActions

file_server_input_data_path = "modules/accelerators/cdap/JoinPlugin/input/"


class TestJoinPlugin(object):
    job_alias = "join_plugin"
    outer_join_job_alias = "outerjoin_plugin"
    guavuspluginversion = "5.3.0-2.0.0"

    @pytest.fixture(scope="session")
    def join_plugin_actions(self):
        return JoinPluginActions(self.job_alias)

    @pytest.fixture(scope="session")
    def outer_join_plugin_actions(self):
        return JoinPluginActions(self.outer_join_job_alias)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/testinnerjoin_v1-cdap-data-pipeline.json",
        file_name_regex="*inner*"))
    def test_validate_innerjoin_plugin(self, user_actions, join_plugin_actions, payload):
        pipeline_name = "automation_testinnerjoin_%s" % NodeManager.current_time
        dataset = "automation_innerjoink_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        join_plugin_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                  guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)
        user_actions.validate(join_plugin_actions.validate_innerjoin_plugin, self.job_alias,
                              dataset_alias="datasetinnerjoin")
        join_plugin_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/TestLeftJoin_v3-cdap-data-pipeline.json",
        file_name_regex="*Left*"))
    def test_validate_leftjoin_plugin(self, user_actions, join_plugin_actions, payload):
        leftpipeline_name = "automation_testleftjoin_%s" % NodeManager.current_time
        leftdataset = "automation_leftjoin_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        join_plugin_actions.add_dynamic_variables(pipeline_name=leftpipeline_name, payload=payload,
                                                  dataset=leftdataset, guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)
        user_actions.validate(join_plugin_actions.validate_leftjoin_plugin, self.job_alias,
                              dataset_alias="datasetleftjoin")
        join_plugin_actions.cdap_utils.delete_pipeline(leftpipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/fullouterjoindeepanshu_v1-cdap-data-pipeline.json",
        file_name_regex="*full*"))
    def test_validate_fullouterjoin_plugin(self, user_actions, outer_join_plugin_actions, payload):
        fullouterpipeline_name = "automation_testfullouterjoin_%s" % NodeManager.current_time
        fullouterdataset = "automation_fullouterjoin_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        outer_join_plugin_actions.add_dynamic_variables(pipeline_name=fullouterpipeline_name, payload=payload,
                                                        dataset=fullouterdataset,
                                                        guavuspluginversion=self.guavuspluginversion)

        user_actions.schedule(self.outer_join_job_alias)
        user_actions.validate(outer_join_plugin_actions.validate_fullouterjoin_plugin, self.outer_join_job_alias,
                              dataset_alias="datasetouterjoin")
        outer_join_plugin_actions.cdap_utils.delete_pipeline(fullouterpipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/incorrect_column_name_of_datastream-cdap-data-pipeline.json",
        file_name_regex="*datastream*"))
    def test_validate_incorrect_column_datastream(self, user_actions, join_plugin_actions, payload):
        incorrectdatastreampipeline_name = "automation_incorrectdatastream_%s" % NodeManager.current_time
        incorrectdatastreamdataset = "automation_incorrectdatastream_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        join_plugin_actions.add_dynamic_variables(pipeline_name=incorrectdatastreampipeline_name, payload=payload,
                                                  dataset=incorrectdatastreamdataset,
                                                  guavuspluginversion=self.guavuspluginversion)
        flag = True
        try:
            user_actions.schedule(self.job_alias)
            flag = False
        except AssertionError:
            assert 1
        if flag is False:
            assert 0, "CDAP pipeline passed when it shouldn't"

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/incorrect_column_name_of_whitelist-cdap-data-pipeline.json",
        file_name_regex="*whitelist*"))
    def test_validate_incorrect_column_whitelist(self, user_actions, join_plugin_actions, payload):
        incorrectwhitelistpipeline_name = "automation_incorrectcolumn_whitelist_%s" % NodeManager.current_time
        incorrectwhitelistdataset = "automation_incorrectcolumn_whitelist_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        join_plugin_actions.add_dynamic_variables(pipeline_name=incorrectwhitelistpipeline_name, payload=payload,
                                                  dataset=incorrectwhitelistdataset,
                                                  guavuspluginversion=self.guavuspluginversion)
        flag = True
        try:
            user_actions.schedule(self.job_alias)
            flag = False
        except AssertionError:
            assert 1
        if flag is False:
            assert 0, "CDAP pipeline passed when it shouldn't"

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/JoinPlugin/pipelines/incorrect_output_schema-cdap-data-pipeline.json",
        file_name_regex="*output_schema*"))
    def test_validate_incorrect_output_schema(self, user_actions, join_plugin_actions, payload):
        incorrectoutputschemapipeline_name = "automation_incorrect_output_schema_%s" % NodeManager.current_time
        incorrectoutputschemadataset = "automation_incorrect_output_schema_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/joinplugin/")
        join_plugin_actions.add_dynamic_variables(pipeline_name=incorrectoutputschemapipeline_name, payload=payload,
                                                  dataset=incorrectoutputschemadataset,
                                                  guavuspluginversion=self.guavuspluginversion)
        flag = True
        try:
            user_actions.schedule(self.job_alias)
            flag = False
        except AssertionError:
            assert 1
        if flag is False:
            assert 0, "CDAP pipeline passed when it shouldn't"

        # response_list=user_actions.schedule(self.job_alias,negative_scenario=True)
        # for response in response_list:
        #     if response.status_code!="Failed":
        #         assert 0,response.status_code
