from nimble.core.utils.components.cdap_utils import CdapUtils

class TestDeletePipelines(object):




    def test_deleteallpipelines(self):

        CdapUtils().delete_all_pipelines()