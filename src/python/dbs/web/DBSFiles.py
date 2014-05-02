"""
DBS 3 Files API
"""

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.dao.Oracle.File.BriefList import BriefList

from dbs.utils.Validation import filename_validation_rx, run_validation_rx
from dbs.utils.DBSTransformInputType import transformInputType, parseRunRange, run_tuple

class Files(RESTEntity):

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.dao = BriefList(api)

    @transformInputType('run_num')
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate input data
        """
        if method=="GET":
            addDefaults(param, detail=False)
            validate_strlist("logical_file_name", param, safe, filename_validation_rx)
            if type(detail) is int:
                validate_num("detail", param, safe, minval=0, maxval=1, optional=True)
            elif type(detail) is bool:
                pass
            #else: do it later
                #throw exception
            run_param = param.kwargs.get('run_num')
            if isinstance(run_param, list):
                ###to simplify input validation, convert every item to a string
                param.kwargs['run_num'] = map(str, run_param)
            validate_strlist("run_num", param, safe, run_validation_rx)

    @restcall
    def get(self, logical_file_name, run_num=-1, detail=False):
        """
        DBS 3 list files API
        """
        return self.dao.execute(logical_file_name=logical_file_name, run_num=run_num, detail=detail)
