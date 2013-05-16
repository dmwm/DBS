"""
DBS 3 Datasets API
"""

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.dao.Oracle.Dataset.List import List

from dbs.utils.Validation import dataset_validation_rx
from dbs.utils.DBSTransformInputType import transformInputType

class Datasets(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.dao = List(api)

    @transformInputType('datasets')
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate input data
        """
        validate_strlist("datasets", param, safe, dataset_validation_rx)

    @restcall
    def get(self, datasets, is_dataset_valid, parent_dataset,
            release_version, pset_hash, app_name, output_module_label,
            processing_version, acquisition_era, run_num,
            physics_group_name, logical_file_name, primary_ds_name,
            primary_ds_type, processed_ds_name, data_tier_name, dataset_access_type,
            prep_id, create_by, last_modified_by, min_cdate, max_cdate, min_ldate,
            max_ldate, cdate, ldate):
        """
        DBS3 list datasets API
        """
        sql_query = """select D.DATASET FROM DATASETS D"""
        #return self.api.query(match=None, select=None, sql=sql_query)
        return self.dao.execute(datasets, is_dataset_valid, parent_dataset,
                                release_version, pset_hash, app_name,
                                output_module_label, processing_version,
                                acquisition_era, run_num, physics_group_name,
                                logical_file_name, primary_ds_name, primary_ds_type,
                                processed_ds_name, data_tier_name, dataset_access_type,
                                prep_id, create_by, last_modified_by, min_cdate,
                                max_cdate, min_ldate, max_ldate, cdate, ldate)
