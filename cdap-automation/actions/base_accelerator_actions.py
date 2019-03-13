from nimble.core import global_constants
from nimble.core.utils.components.cdap_utils import CdapUtils
from nimble.core.utils.dynamic_substitution_utils import DynamicSubstitutionUtils
from nimble.core.utils.file_server_utils import FileServerUtils
from nimble.core.utils.shell_utils import ShellUtils


class BaseAcceleratorActions(object):
    def __init__(self):
        self.file_server_utils = FileServerUtils()
        self.cdap_utils = CdapUtils()

    @staticmethod
    def fetch_from_file_server(file_server_payload_path, file_name_regex=None):
        payload_list = []
        local_payload_path = "%s/tmp_pipeline_payloads/" % global_constants.DEFAULT_LOCAL_ARTIFACTS_PATH
        assert ShellUtils.execute_shell_command(
            ShellUtils.create_directory(local_payload_path)).status is True
        FileServerUtils().download(file_server_payload_path, local_payload_path)
        payload_list.extend(ShellUtils.execute_shell_command(
            ShellUtils.find_files_in_directory(local_payload_path,
                                               file_name_regex=file_name_regex)).stdout.strip().split("\n"))
        return payload_list

    @staticmethod
    def add_dynamic_variables(**kwargs):
        pipeline_name = kwargs["pipeline_name"]
        payload = kwargs["payload"]
        dataset = kwargs.get("dataset", None)
        CDAPversion = "5.1.0"
        guavuspluginversion = kwargs.get("guavuspluginversion", None)
        # filemarketplaceplugin = "1.8.4"
        filemarketplaceplugin = "2.1.1-SNAPSHOT"
        # wranglermarketplaceplugin = "3.0.4"
        wranglermarketplaceplugin = "3.2.0"
        # CDAPdatasetmarketplaceversion = "1.8.4"
        CDAPdatasetmarketplaceversion = "2.1.1-SNAPSHOT"

        DynamicSubstitutionUtils.add(
            {"randynamic_pipeline_payload": payload, "randynamic_pipeline_name": pipeline_name,
             "randynamic_dataset": dataset, "randynamic_CDAPversion": CDAPversion,
             "randynamic_version": guavuspluginversion, "randynamic_filebatchversion": filemarketplaceplugin,
             "randynamic_wranglerversion": wranglermarketplaceplugin,
             "randynamic_CDAPdatasetversion": CDAPdatasetmarketplaceversion})
