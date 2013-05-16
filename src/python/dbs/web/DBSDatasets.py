"""
DBS 3 Datasets API
"""

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.dao.Oracle.Dataset.List import List

from dbs.utils.Validation import *
from dbs.utils.DBSTransformInputType import transformInputType, parseRunRange, run_tuple

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
        validate_num("is_dataset_valid", param, safe, minval=0, maxval=1, optional=True)
        validate_str("parent_dataset", param, safe, dataset_validation_rx, optional=True)
        validate_str("release_version", param, safe, release_version_validation_rx, optional=True)
        validate_str("pset_hash", param, safe, string_validation_rx, optional=True)
        validate_str("app_name", param, safe, string_validation_rx, optional=True)
        validate_str("output_module_label", param, safe, string_validation_rx, optional=True)
        validate_str("processing_version", param, safe, processing_version_validation_rx, optional=True)
        validate_str("acquisition_era", param, safe, acquisition_era_validation_rx, optional=True)
        run_param = param.kwargs.get('run_num')
        if isinstance(run_param, list):
            ###to simplify input validation, convert every item to a string
            param.kwargs['run_num'] = map(str, run_param)
        validate_strlist("run_num", param, safe, run_validation_rx)
        validate_str("physics_group_name", param, safe, string_validation_rx, optional=True)
        validate_str("logical_file_name", param, safe, filename_validation_rx, optional=True)
        validate_str("primary_ds_name", param, safe, primary_ds_name_validation_rx, optional=True)
        validate_str("primary_ds_type", param, safe, string_validation_rx, optional=True)
        validate_str("processed_ds_name", param, safe, processed_ds_name_validation_rx, optional=True)
        validate_str("data_tier_name", param, safe, data_tier_name_validation_rx, optional=True)
        validate_str("dataset_access_type", param, safe, string_validation_rx, optional=True)
        validate_num("prep_id", param, safe, optional=True)
        validate_str("create_by", param, safe, string_validation_rx, optional=True)
        validate_str("last_modified_by", param, safe, string_validation_rx, optional=True)
        validate_num("min_cdate", param, safe, optional=True, minval=0)
        validate_num("max_cdate", param, safe, optional=True, minval=0)
        validate_num("min_ldate", param, safe, optional=True, minval=0)
        validate_num("max_ldate", param, safe, optional=True, minval=0)
        validate_num("cdate", param, safe, optional=True, minval=0)
        validate_num("ldate", param, safe, optional=True, minval=0)

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
