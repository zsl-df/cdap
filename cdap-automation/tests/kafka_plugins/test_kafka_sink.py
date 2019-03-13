import time

import pytest
from nimble.core import global_constants
from nimble.core.entity.node_manager import NodeManager
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils
from nimble.core.utils.file_server_utils import FileServerUtils

from actions.base_accelerator_actions import BaseAcceleratorActions
from actions.kafka_plugins.kafka_plugins_actions import KafkaSourceActions

file_server_payload_path = "modules/accelerators/cdap/kafka_source/pipelines/"
file_server_input_data_path = "modules/accelerators/cdap/kafka_sink/input/"


class TestKafkaSource(object):
    job_alias_json = "kafka_sink_json"
    guavuspluginversion = "5.3.0-2.0.0"
    job_alias_csv = "kafka_sink_csv"
    job_alias_avro = "kafka_sink_registry_less_avro"

    @pytest.fixture(scope="session")
    def kafka_source_actions(self):
        return KafkaSourceActions(self.job_alias_json)

    @pytest.fixture(scope="session")
    def kafka_source_actions_csv(self):
        return KafkaSourceActions(self.job_alias_csv)

    @pytest.fixture(scope="session")
    def kafka_source_actions_registry_less_avro(self):
        return KafkaSourceActions(self.job_alias_avro)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_sink/pipelines/kafkasinkcsv-cdap-data-pipeline.json",
        file_name_regex="*sinkcsv*"))
    def test_validate_kafka_sink_csv(self, user_actions, kafka_source_actions_csv, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Validate that CDAP plugin must insert the data into kafka the with csv format.
        """
        pipeline_name = "automation_kafka_sink_csv_%s" % NodeManager.current_time
        kafka_topic = "automation_kafka_sink_topic_csv_%s" % NodeManager.current_time
        dataset = "automation_kafka_sink__dataset_csv_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/kafka_sink/")
        DynamicSubstitutionUtils.add({"randynamic_topic": kafka_topic,
                                      "randynamic_brokers": kafka_source_actions_csv.kafka_utils.bootstrap_servers_string})
        kafka_source_actions_csv.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                       guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias_csv, capsys)

        user_actions.validate(kafka_source_actions_csv.validate_kafka_sink_csv, self.job_alias_csv, capsys)
        kafka_source_actions_csv.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions_csv.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_sink/pipelines/kafkasinkjson-cdap-data-pipeline.json",
        file_name_regex="*sinkjson*"))
    def test_validate_kafka_sink_json(self, user_actions, kafka_source_actions, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Validate that CDAP plugin must insert the data into kafka the with json format.
        """
        pipeline_name = "automation_kafka_sink_json_%s" % NodeManager.current_time
        kafka_topic = "automation_kafka_sink_topic_json_%s" % NodeManager.current_time
        dataset = "automation_kafka_sink__dataset_json_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/kafka_sink/")
        DynamicSubstitutionUtils.add({"randynamic_topic": kafka_topic,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias_json, capsys)

        user_actions.validate(kafka_source_actions.validate_kafka_sink, self.job_alias_json, capsys)
        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_sink/pipelines/kafkasinkavro-cdap-data-pipeline.json",
        file_name_regex="*sinkavro*"))
    def test_validate_kafka_sink_avro(self, user_actions, kafka_source_actions_registry_less_avro, payload):
        """
        Functional Test Cases to be covered:-

        1.) Validate that CDAP plugin must insert the data into kafka the with avro format.
        """
        pipeline_name = "automation_kafka_sink_avro_%s" % NodeManager.current_time
        topic = "automation_kafka_sink_topic_avro_%s" % NodeManager.current_time
        dataset = "automation_kafka_sink__dataset_avro_%s" % NodeManager.current_time
        user_actions.send_data_to_hdfs(file_server_input_data_path, "/tmp/kafka_sink/")
        DynamicSubstitutionUtils.add({"randynamic_topic": topic,
                                      "randynamic_brokers": kafka_source_actions_registry_less_avro.kafka_utils.bootstrap_servers_string})
        kafka_source_actions_registry_less_avro.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload,
                                                                      dataset=dataset,
                                                                      guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias_avro)

        user_actions.validate(kafka_source_actions_registry_less_avro.validate_kafka_sink, self.job_alias_avro)
        kafka_source_actions_registry_less_avro.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions_registry_less_avro.cdap_utils.delete_pipeline(pipeline_name)
