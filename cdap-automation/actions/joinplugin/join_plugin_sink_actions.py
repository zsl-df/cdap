from actions.base_accelerator_actions import BaseAcceleratorActions
from nimble.core import global_constants
from nimble.core.parser.json_parser import JsonParser
from nimble.core.utils.components.kafka_utils import KafkaUtils
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils
from nimble.core.utils.shell_utils import ShellUtils


class JoinPluginActions(BaseAcceleratorActions):
    def __init__(self, job_alias):
        super(JoinPluginActions, self).__init__()
        self.tmp_input_path = "%s/tmp_input/" % global_constants.DEFAULT_LOCAL_TMP_PATH
        self.file_server_input_path = "modules/accelerators/cdap/JoinPlugin/input/"
        self.job_alias = job_alias
        ShellUtils.execute_shell_command(ShellUtils.remove_and_create_directory(self.tmp_input_path))
        self.inputdataset="""ifnull("#sn-end-time","") as "_sn_end_time",
            ifnull("sn-start-time","") as "sn_start_time",
            ifnull("radius-called-station-id","") as "radius_called_station_id",
            ifnull("transaction-uplink-bytes","") as "transaction_uplink_bytes",
            ifnull("transaction-downlink-bytes","") as "transaction_downlink_bytes",
            ifnull("ip-subscriber-ip-address","") as "ip_subscriber_ip_address",
            ifnull("ip-server-ip-address","") as "ip_server_ip_address",
            ifnull("tcp-os-signature","") as "tcp_os_signature",
            ifnull("tcp-v6-os-signature","") as "tcp_v6_os_signature",
            ifnull("http-host","") as "http_host",
            ifnull("http-content_type","") as "http_content_type",
            ifnull("http-url-2000","") as "http_url_2000",
            ifnull("http-reply code","") as "http_reply_code",
            ifnull("http-user-agent","") as "http_user_agent",
            ifnull("bearer-3gpp_imei","") as "bearer_3gpp_imei",
            ifnull("bearer-3gpp rat-type","") as "bearer_3gpp_rat_type",
            ifnull("tethered","") as "tethered",
            ifnull("http-referer","") as "http_referer",
            ifnull("http-request method","") as "http_request_method",
            ifnull("bearer-3gpp imsi","") as "bearer_3gpp_imsi",
            ifnull("sn-flow-id","") as "sn_flow_id",
            ifnull("ttl","") as "ttl",
            ifnull("ip-protocol","") as "ip_protocol",
            ifnull("p2p-protocol","") as "p2p_protocol",
            ifnull("sn-charge-volume-ip-bytes-downlink","") as "sn_charge_volume_ip_bytes_downlink",
            ifnull("sn-charge-volume-ip-bytes-uplink","") as "sn_charge_volume_ip_bytes_uplink",
            ifnull("tcp-flag","") as "tcp_flag",
            ifnull("tcp-previous-state","") as "tcp_previous_state",
            ifnull("tcp-state","") as "tcp_state",
            ifnull("tethered-ip-ttl","") as "tethered_ip_ttl",
            ifnull("event-label","") as "event_label",
            ifnull("sn-subscriber-port","") as "sn_subscriber_port",
            ifnull("sn-server-port","") as "sn_server_port",
            ifnull("RequestHeader.DNT_x-do-not-track","") as "requestheader_dnt_x_do_not_track",
            ifnull("sn-duration","") as "sn_duration",
            ifnull("http-content length","") as "http_content_length",
            ifnull("bearer-3gpp user-location-information","") as "bearer_3gpp_user_location_information",
            ifnull("radius-calling-station-id","") as "radius_calling_station_id",
            ifnull(URL, "") as url
            """
    def validate_innerjoin_plugin(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select %(edrdata)s from join_plugin_input1 inner join join_plugin_input2 on 
            join_plugin_input1."http-content_type" = join_plugin_input2.URL""" % {"job": self.job_alias,"edrdata":self.inputdataset})

    def validate_leftjoin_plugin(self, validation_entities):
        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select %(edrdata)s from %(job)s_input1 left join %(job)s_input2 on 
            %(job)s_input1."http-content_type" = %(job)s_input2.URL""" % {"job": self.job_alias,"edrdata":self.inputdataset})


    def validate_fullouterjoin_plugin(self, validation_entities):


        validation_entities.output_obj[self.job_alias]["output1"] = validation_entities.sqlite_adapter.select(
            """select %(edrdata)s, "abc"
            from %(job)s_input1 left join %(job)s_input2 on 
            %(job)s_input1."http-content_type" = %(job)s_input2.URL
            union all
            select %(edrdata)s, "abc"
            from %(job)s_input2 left join %(job)s_input1 on 
            %(job)s_input1."http-content_type" = %(job)s_input2.URL where
             %(job)s_input1."http-content_type" is null""" % {"job": self.job_alias,"edrdata":self.inputdataset})
