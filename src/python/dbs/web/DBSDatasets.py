"""
DBS 3 Datasets API
"""

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.utils.Validation import dataset_validation_rx
from dbs.utils.DBSTransformInputType import transformInputType

class Datasets(RESTEntity):
    @transformInputType('datasets')
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate input data
        """
        validate_strlist("datasets", param, safe, dataset_validation_rx)

    @restcall
    def get(self, datasets):
        """
        DBS3 list datasets API
        """
        sql_query = """select D.DATASET FROM DATASETS D"""
        return self.api.query(match=None, select=None, sql=sql_query)
