import time

import pytest
from nimble.core.entity.node_manager import NodeManager

from actions.base_accelerator_actions import BaseAcceleratorActions
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils

from actions.elasticsearch.elastic_search_sink_actions import ElasticDataSinkActions

file_server_input_data_path = "modules/accelerators/cdap/elasticsearch/input/"


class TestElasticSearchSink(object):
    job_alias = "elastic_search_sink"
    guavuspluginversion = "5.3.0-2.0.0"

    @pytest.fixture(scope="session")
    def elastic_search_actions(self):
        return ElasticDataSinkActions(self.job_alias)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/elasticsearch/pipelines/elastic_batch_sink_withid-cdap-data-pipeline.json",
        file_name_regex="*withid*"))
    def test_validate_elastic_search_sink_id(self, user_actions, elastic_search_actions, payload):
        pipeline_name = "automation_elasticsearch_sink_id_%s" % NodeManager.current_time
        elastic_search_host = "10.233.0.10:9201"
        index = "automation_index_id_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/elasticsearchsink/")
        DynamicSubstitutionUtils.add({"randynamic_host": elastic_search_host, "randynamic_index": index})
        elastic_search_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload,
                                                     guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)

        user_actions.validate(elastic_search_actions.validate_elastic_search_sink, self.job_alias,
                              dataset_alias="sinkid")
        elastic_search_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/elasticsearch/pipelines/elastic_batch_sink_withoutid-cdap-data-pipeline.json",
        file_name_regex="*withoutid*"))
    def test_validate_elastic_search_sink_withoutid(self, user_actions, elastic_search_actions, payload):
        pipeline_name = "automation_elasticsearch_sink_withoutid_%s" % NodeManager.current_time
        elastic_search_host = "10.233.0.10:9201"
        index = "automation_index_without_id_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/elasticsearchsink/")
        DynamicSubstitutionUtils.add({"randynamic_host": elastic_search_host, "randynamic_index": index})
        elastic_search_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload,
                                                     guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias)

        user_actions.validate(elastic_search_actions.validate_elastic_search_sink, self.job_alias,
                              dataset_alias="sinkwithoutid")
        elastic_search_actions.cdap_utils.delete_pipeline(pipeline_name)
