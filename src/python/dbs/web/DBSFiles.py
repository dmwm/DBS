"""
DBS 3 Files API
"""

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.utils.Validation import filename_validation_rx, run_validation_rx
from dbs.utils.DBSTransformInputType import transformInputType, parseRunRange

class Files(RESTEntity):
    @transformInputType('logical_file_names', 'run')
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate input data
        """
        validate_strlist("logical_file_names", param, safe, filename_validation_rx)
        validate_strlist("run", param, safe, run_validation_rx)
        validate_num("summary", param, safe, optional=True, minval=0, maxval=1)

    @restcall
    def get(self, logical_file_names, run, summary):
        """
        DBS 3 list files API
        """
        lfn='/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8d932f3d-fac6-4616-b833-336e3f695553_6.root'
        sql_query = """select F.LOGICAL_FILE_NAME FROM FILES F WHERE LOGICAL_FILE_NAME=:logical_file_name"""
        binds = {'logical_file_name' : lfn}
        return self.api.query(match=None, select=None, sql=sql_query, **binds)
