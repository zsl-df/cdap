import time

import pytest
from nimble.core.entity.node_manager import NodeManager
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils

from actions.base_accelerator_actions import BaseAcceleratorActions
from actions.kafka_plugins.kafka_plugins_actions import KafkaSourceActions

file_server_payload_path = "modules/accelerators/cdap/kafka_source/pipelines/"


class TestKafkaSource(object):
    job_alias = "kafka_source"
    # randynamic_version="5.3.0-2.0.0"
    guavuspluginversion = "5.3.0-2.0.0"
    job_alias_text = "kafka_source_text"

    @pytest.fixture(scope="session")
    def kafka_source_actions(self):
        return KafkaSourceActions(self.job_alias)

    @pytest.fixture(scope="session")
    def kafka_source_actions_text(self):
        return KafkaSourceActions(self.job_alias_text)

    @pytest.mark.skip("Not automable")
    def test_validate_cdap_ui(self):
        """
        Functional Testcases to be covered

        1.) Validate that kafka source plugin should be in the source category after running the ansible script.
        2.) Verify that plugin should have proper description in CDAP reference page
        3.) Verify that placeholder and hovering should be correct.
        4.) Verify that plugin should not work in case any mandatory parameter is not present.
        5.) Verify functionality of Get schema button.
        6.) Verify functionality of Max rate per partition.
        7.) Verify Get schema should not work with empty schema registry url.
        8.) Verify Get schema should not work when schema registry url is not reachable.
        """

    @pytest.mark.skip("Not automable")
    def test_validate_cdap_logging(self):
        """
        Functional Testcases to be covered

        1.) Verify that proper logging should be there in case of failure as well as success.
        2.) Verify that proper stats reporting should be there in the output for plugin like how many
         records processed, how many dropped (with reason).
        """

    @pytest.mark.skip("Not automated")
    def test_validate_kafka_source_performance(self):
        """
        Functional Testcases to be covered

        1.) Verify the pipeline with some performance data and see whether the records are getting processed or not.
        """

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/kafkaavrowithschema-cdap-data-streams.json",
        file_name_regex="*avrowithschema*"))
    @pytest.mark.skip("Not automated")
    def test_validate_kafka_with_hortonworks_schema_registry(self, user_actions, kafka_source_actions, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Validate CDAP plugin must be read the data from the kafka using the hortonworks schema registry and
        write it into the Cdap dataset.
        2.) Validate CDAP plugin must be read the data from the kafka  when optional field is empty and
        write it into the Cdap dataset.
        """
        pipeline_name = "automation_kafka_source_with_hortonworks%s" % NodeManager.current_time
        topic = "automation_kafka_source_with_hortonworks%s" % NodeManager.current_time
        dataset = "automation_kafka_source_dataset_%s" % NodeManager.current_time
        DynamicSubstitutionUtils.add({"randynamic_topic": topic, "randynamic_schemaregistry": NodeManager.node_obj.vip,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(150)
        kafka_source_actions.send_data_to_kafka(topic)
        user_actions.validate(kafka_source_actions.validate_kafka_source, self.job_alias, capsys)
        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.skip("Not automated")
    def test_verify_topic_partitions(self):
        """
        Functional Test Cases to be covered:-

        1.) Verify the functionality of the topic partitions.

        """

    @pytest.mark.skip("Not automated")
    def test_verify_initial_partition_offset(self):
        """
        Functional Test Cases to be covered:-

        1.) Verify the functionality of the Initial Partition Offsets.

        """

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/Kafka_source_allfields-cdap-data-streams.json",
        file_name_regex="*allfields*"))
    def test_verify_kafka_fields(self, user_actions, kafka_source_actions, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Verify the Time field,key field ,Partition Field and offset field.

        """
        pipeline_name = "automation_kafka_source_allfields_%s" % NodeManager.current_time
        topic = "automation_kafka_source_allfields_%s" % NodeManager.current_time
        dataset = "automation_kafka_source_allfields_dataset"
        DynamicSubstitutionUtils.add({"randynamic_topic": topic, "randynamic_schemaregistry": NodeManager.node_obj.vip,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(120)
        kafka_source_actions.send_data_to_kafka(topic)

        try:
            user_actions.validate(kafka_source_actions.validate_kafka_source, self.job_alias, capsys,
                                  dataset_alias="kafka_all_fields")

        except AssertionError:
            assert 1

        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/test_kafka_schemaUrlEmpty-cdap-data-streams.json",
        file_name_regex="*schemaUrlEmpty*"))
    def test_verify_schema_registry_empty_schema_name_non_empty(self, user_actions, kafka_source_actions, payload,
                                                                capsys):
        """
        Functional Test Cases to be covered:-

        1.) Verify that if schema registry url is empty and and schema Name is non empty then, pipeline should be failed.

        """

        pipeline_name = "automation_kafka_source_empty_schema_url_%s" % NodeManager.current_time
        topic = "automation_kafka_source_empty_schema_url_%s" % NodeManager.current_time
        dataset = "automation_kafka_source_dataset_empty_schema_url_%s" % NodeManager.current_time
        DynamicSubstitutionUtils.add({"randynamic_topic": topic, "randynamic_schemaregistry": NodeManager.node_obj.vip,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        kafka_source_actions.send_data_to_kafka(topic)

        try:
            user_actions.schedule(self.job_alias, capsys)

        except AssertionError:
            assert 1

        time.sleep(120)
        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/testkafkatext-cdap-data-streams.json",
        file_name_regex="*text*"))
    def test_verify_kafka_with_text(self, user_actions, kafka_source_actions_text, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Verify that if schema registry url is empty and and schema Name is non empty then, pipeline should be failed.
        2.) Verify the functionality of the Default initial offset.

        """
        pipeline_name = "automation_kafka_source_withtext_%s" % NodeManager.current_time
        topic = "automation_kafka_source_with_text_%s" % NodeManager.current_time
        dataset = "automation_kafka_source_dataset_text_%s" % NodeManager.current_time
        DynamicSubstitutionUtils.add({"randynamic_topic": topic,
                                      "randynamic_brokers": kafka_source_actions_text.kafka_utils.bootstrap_servers_string})
        kafka_source_actions_text.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                        guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(130)
        kafka_source_actions_text.send_data_to_kafka_with_text(topic)
        user_actions.validate(kafka_source_actions_text.validate_kafka_source_with_text, self.job_alias_text, capsys,
                              dataset_alias="kafka_txt")
        kafka_source_actions_text.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions_text.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/kafkaavrowithoutschema-cdap-data-streams.json",
        file_name_regex="*withoutschema*"))
    def test_verify_kafka_avro_without_schema(self, user_actions, kafka_source_actions, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) 1.) Verify that kafka source takes the data without schema registry url.

        """
        pipeline_name = "automation_kafka_source_avro_withoutschema_%s" % NodeManager.current_time
        topic = "automation_kafka_source_avro_withoutschema_%s" % NodeManager.current_time
        dataset = "automation_kafka_source_avro_withoutschema_dataset_%s" % NodeManager.current_time
        DynamicSubstitutionUtils.add({"randynamic_topic": topic,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(200)
        kafka_source_actions.send_data_to_kafka_without_hortonworks(topic)
        user_actions.validate(kafka_source_actions.validate_kafka_source, self.job_alias, capsys,
                              dataset_alias="kafka_avro_less_schema")
        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)

    @pytest.mark.parametrize("payload", BaseAcceleratorActions.fetch_from_file_server(
        "modules/accelerators/cdap/kafka_source/pipelines/testkafkasource_bytes-cdap-data-streams.json",
        file_name_regex="*bytes*"))
    def test_verify_kafka_with_bytes(self, user_actions, kafka_source_actions, payload, capsys):
        """
        Functional Test Cases to be covered:-

        1.) Verify that if schema registry url is empty and and schema Name is non empty then, pipeline should be failed.

        """
        pipeline_name = "automation_kafka_source_with_bytes_%s" % NodeManager.current_time
        topic = "automation_kafka_source_with_bytes_%s" % NodeManager.current_time
        dataset = "automation_kafka_source_dataset_bytes_%s" % NodeManager.current_time
        DynamicSubstitutionUtils.add({"randynamic_topic": topic,
                                      "randynamic_brokers": kafka_source_actions.kafka_utils.bootstrap_servers_string})
        kafka_source_actions.add_dynamic_variables(pipeline_name=pipeline_name, payload=payload, dataset=dataset,
                                                   guavuspluginversion=self.guavuspluginversion)
        user_actions.schedule(self.job_alias, capsys)
        time.sleep(130)
        kafka_source_actions.send_data_to_kafka_with_text(topic)

        try:
            user_actions.validate(kafka_source_actions.validate_kafka_source, self.job_alias, capsys,
                                  dataset_alias="kafka_bytes")

        except AssertionError:
            assert 1

        kafka_source_actions.cdap_utils.stop_pipeline(pipeline_name)
        kafka_source_actions.cdap_utils.delete_pipeline(pipeline_name)
