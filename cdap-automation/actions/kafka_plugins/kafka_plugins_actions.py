from actions.base_accelerator_actions import BaseAcceleratorActions
from nimble.core import global_constants
from nimble.core.parser.json_parser import JsonParser
from nimble.core.utils.components.kafka_utils import KafkaUtils
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils
from nimble.core.utils.shell_utils import ShellUtils


class KafkaSourceActions(BaseAcceleratorActions):
    def __init__(self, job_alias):
        super(KafkaSourceActions, self).__init__()
        self.kafka_utils = KafkaUtils()
        self.tmp_input_path = "%s/tmp_input/" % global_constants.DEFAULT_LOCAL_TMP_PATH
        self.file_server_input_path = "modules/accelerators/cdap/kafka_source/input/"
        self.job_alias = job_alias
        ShellUtils.execute_shell_command(ShellUtils.remove_and_create_directory(self.tmp_input_path))

    def get_kafka_schema(self):
        schema_file_name = "schema.json"
        self.file_server_utils.download("%s/%s" % (self.file_server_input_path, schema_file_name),
                                        path_to_download=self.tmp_input_path)
        return JsonParser.parse_json("%s/%s" % (self.tmp_input_path, schema_file_name))

    def send_data_to_kafka(self, topic):
        input_file_name = "differendatatypes.txt"
        self.file_server_utils.download("%s/%s" % (self.file_server_input_path, input_file_name),
                                        path_to_download=self.tmp_input_path)
        self.kafka_utils.import_file(topic, "%s/%s" % (self.tmp_input_path, input_file_name),
                                     value_serializer=Serializer.AVRO.value,
                                     value_schema=self.get_kafka_schema())

    def send_data_to_kafka_with_text(self, topic):
        input_file_name = "differendatatypes.txt"
        self.file_server_utils.download("%s/%s" % (self.file_server_input_path, input_file_name),
                                        path_to_download=self.tmp_input_path)
        self.kafka_utils.import_file(topic, "%s/%s" % (self.tmp_input_path, input_file_name))

    def send_data_to_kafka_without_hortonworks(self, topic):
        input_file_name = "differendatatypes.txt"
        self.file_server_utils.download("%s/%s" % (self.file_server_input_path, input_file_name),
                                        path_to_download=self.tmp_input_path)

        self.kafka_utils.import_file(topic, "%s/%s" % (self.tmp_input_path, input_file_name),
                                     value_serializer=Serializer.REGISTRY_LESS_AVRO.value,
                                     value_schema=self.get_kafka_schema())

    def validate_kafka_source(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select str1,age_student,marks,address from %(job)s_input1""" % {"job": self.job_alias})

    def validate_kafka_source_with_text(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output_text"] = validation_entities.sqlite_adapter.select(
            """select * from %(job)s_input_text""" % {"job": self.job_alias})

        validation_entities.actual_output_obj[self.job_alias][
            "output_text"] = validation_entities.sqlite_adapter.select(
            """select c_1 from %(job)s_output_text_kafka_txt""" % {"job": self.job_alias})


    def validate_kafka_sink(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select str1,address,age_student,marks from %(job)s_input1""" % {"job": self.job_alias})


    def validate_kafka_sink_csv(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select str1,age_student,marks,address from %(job)s_input1""" % {"job": self.job_alias})