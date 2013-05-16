#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
from cherrypy import log
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(object):
    """
    Dataset List DAO class.
    """
    def __init__(self, api):
        """
        Add api, logger and basesql
        """
        self.api = api
        self.logger = log.error_log
	self.basesql = \
	"""
	D.DATASET_ID, D.DATASET, D.PREP_ID,
        D.XTCROSSSECTION,
        D.CREATION_DATE, D.CREATE_BY,
        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
        P.PRIMARY_DS_NAME,
        PDT.PRIMARY_DS_TYPE,
        PD.PROCESSED_DS_NAME,
        DT.DATA_TIER_NAME,
        DP.DATASET_ACCESS_TYPE,
        AE.ACQUISITION_ERA_NAME,
        PE.PROCESSING_VERSION,
        PH.PHYSICS_GROUP_NAME

	FROM DATASETS D
	JOIN PRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
	JOIN PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
	JOIN PROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
	JOIN DATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
	JOIN DATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID

	LEFT OUTER JOIN ACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
	LEFT OUTER JOIN PROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
	LEFT OUTER JOIN PHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
	"""

    def execute(self, dataset="", is_dataset_valid=1, parent_dataset="",\
                release_version="", pset_hash="", app_name="", output_module_label="",\
                processing_version=0, acquisition_era="", run_num=0,\
                physics_group_name="", logical_file_name="", primary_ds_name="",\
                primary_ds_type="", processed_ds_name="", data_tier_name="", dataset_access_type="", prep_id="",\
                create_by='', last_modified_by='', min_cdate=0, max_cdate=0, min_ldate=0, max_ldate=0, cdate=0,\
                ldate=0):
	sql = ""
	basesql=self.basesql
        binds = {}
	wheresql = "WHERE D.IS_DATASET_VALID = :is_dataset_valid "
        if dataset and type(dataset) is list:  # for the POST method
            wheresql += " AND D.DATASET=:dataset "
            if dataset_access_type and dataset_access_type !="%":
                op = ("=", "like")["%" in dataset_access_type]
                wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type " %op
                binds = [{'dataset_access_type':dataset_access_type, 'is_dataset_valid':is_dataset_valid, 'dataset': x } for x in dataset]
                self.logger.debug(binds)
            else:
                binds = [{'is_dataset_valid':is_dataset_valid, 'dataset': x } for x in dataset]
            sql ='SELECT' + basesql + wheresql
        else: #for the GET method
            binds.update(is_dataset_valid = is_dataset_valid)
            if cdate != 0:
                wheresql += "AND D.CREATION_DATE = :cdate "
                binds.update(cdate = cdate)
            elif min_cdate != 0 and max_cdate != 0:
                wheresql += "AND D.CREATION_DATE BETWEEN :min_cdate and :max_cdate "
                binds.update(min_cdate = min_cdate)
                binds.update(max_cdate = max_cdate)
            elif min_cdate != 0 and max_cdate == 0:
                wheresql += "AND D.CREATION_DATE > :min_cdate "
                binds.update(min_cdate = min_cdate)
            elif min_cdate ==0 and max_cdate != 0:
                wheresql += "AND D.CREATION_DATE < :max_cdate "
                binds.update(max_cdate = max_cdate)
            else:
                pass
            if ldate != 0:
                wheresql += "AND D.LAST_MODIFICATION_DATE = :ldate "
                binds.update(ldate = ldate)
            elif min_ldate != 0 and max_ldate != 0:
                wheresql += "AND D.LAST_MODIFICATION_DATE BETWEEN :min_ldate and :max_ldate "
                binds.update(min_ldate = min_ldate)
                binds.update(max_ldate = max_ldate)
            elif min_ldate != 0 and max_ldate == 0:
                wheresql += "AND D.LAST_MODIFICATION_DATE > :min_ldate "
                binds.update(min_ldate = min_ldate)
            elif min_cdate ==0 and max_cdate != 0:
                wheresql += "AND D.LAST_MODIFICATION_DATE < :max_ldate "
                binds.update(max_ldate = max_ldate)
            else:
                pass
            if create_by:
                wheresql +=  " AND D.CREATE_BY = :create_by "
                binds.update(create_by = create_by)
            if last_modified_by:
                wheresql += " AND D.LAST_MODIFIED_BY = :last_modified_by "
                binds.update(last_modified_by = last_modified_by)
            if prep_id:
                wheresql += "AND D.PREP_ID = :prep_id "
                binds.update(prep_id = prep_id)
            if dataset and dataset != "%":
               op = ("=", "like")["%" in dataset]
               wheresql += " AND D.DATASET %s :dataset " % op
               binds.update(dataset = dataset)
            if primary_ds_name and primary_ds_name != "%":
               op = ("=", "like")["%" in primary_ds_name ]
               wheresql += " AND P.PRIMARY_DS_NAME %s :primary_ds_name " % op
               binds.update(primary_ds_name = primary_ds_name)
            if processed_ds_name and processed_ds_name != "%":
                joinsql += " JOIN PROCESSED_DATASETS PR ON PR.PROCESSED_DS_ID = D.PROCESSED_DS_ID "
                op = ("=", "like")["%" in processed_ds_name ]
                wheresql += " AND PR.PROCESSED_DS_NAME %s :processed_ds_name " % op
                binds.update(processed_ds_name = processed_ds_name)
            if data_tier_name and data_tier_name != "%":
               op = ("=", "like")["%" in data_tier_name ]
               wheresql += " AND DT.DATA_TIER_NAME %s :data_tier_name " % op
               binds.update(data_tier_name=data_tier_name)
            if dataset_access_type and dataset_access_type !="%":
               op = ("=", "like")["%" in dataset_access_type]
               wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type " %op
               binds.update(dataset_access_type=dataset_access_type)
            if primary_ds_type and  primary_ds_type !="%":
               op = ("=", "like")["%" in primary_ds_type]
               wheresql += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type " %op
               binds.update(primary_ds_type=primary_ds_type)
            if physics_group_name and physics_group_name !="%":
               op = ("=", "like")["%" in physics_group_name]
               wheresql += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name " %op
               binds.update(physics_group_name=physics_group_name)

            if parent_dataset:

                basesql = "PDS.DATASET PARENT_DATASET, " + basesql
                basesql += """
    		LEFT OUTER JOIN DATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
    		LEFT OUTER JOIN DATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID
    		"""
                wheresql += " AND PDS.DATASET = :parent_dataset "
                binds.update(parent_dataset = parent_dataset)

            if release_version or pset_hash or app_name or output_module_label:
                basesql = """
    			 OMC.OUTPUT_MODULE_LABEL,
    			 RV.RELEASE_VERSION,
    			 PSH.PSET_HASH,
    			 AEX.APP_NAME, """ + basesql

                basesql += """
    		LEFT OUTER JOIN DATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
    		LEFT OUTER JOIN OUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
    		LEFT OUTER JOIN RELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
    		LEFT OUTER JOIN PARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
    		LEFT OUTER JOIN APPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID
                """

            if release_version:
                op = ("=", "like")["%" in release_version]
                wheresql += " AND RV.RELEASE_VERSION %s :release_version " % op
                binds.update(release_version=release_version)
            if pset_hash:
                op = ("=", "like")["%" in pset_hash]
                wheresql += " AND PSH.PSET_HASH %s :pset_hash " % op
                binds.update(pset_hash = pset_hash)
            if app_name:
                op = ("=", "like")["%" in app_name]
                wheresql += " AND AEX.APP_NAME %s :app_name " % op
                binds.update(app_name = app_name)
            if output_module_label:
                op = ("=", "like")["%" in output_module_label]
                wheresql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label " % op
                binds.update(output_module_label=output_module_label)
            if processing_version != 0:
                #op = ("=", "like")["%" in processing_version]
                op = "="
                wheresql += " AND PE.PROCESSING_VERSION %s :pversion " % op
                binds.update(pversion=processing_version)
            if acquisition_era:
                op = ("=", "like")["%" in acquisition_era]
                wheresql += " AND AE.ACQUISITION_ERA_NAME %s :aera " % op
                binds.update(aera=acquisition_era)

            # This should resolve to original cases that were in the business logic
            if (not logical_file_name or  logical_file_name=="%") and (not run_num or run_num==0):
    		# """JUST EXECUTE THE QUERY HERE"""
                sql = "SELECT " + basesql + wheresql

            elif (not run_num or run_num==0) and logical_file_name and logical_file_name !="%":
                # """DO execute 1 thingy"""
    		sql = "SELECT DISTINCT " + basesql
    		sql += " JOIN FILES FL on FL.DATASET_ID = D.DATASET_ID "
    		wheresql += " AND FL.LOGICAL_FILE_NAME = :logical_file_name "
    		binds.update(logical_file_name = logical_file_name)
    		sql += wheresql

            elif(run_num and run_num!=0):
                # """Do execute 2 thingy"""
    		sql += "SELECT DISTINCT " + basesql
    		if logical_file_name:
    			sql += "JOIN FILES FL on FL.DATASET_ID = D.DATASET_ID "
    			wheresql += " AND FL.LOGICAL_FILE_NAME = :logical_file_name "
    			binds.update(logical_file_name = logical_file_name)
    		else:
    		    sql += " JOIN FILES FL on FL.DATASET_ID = D.DATASET_ID "
                sql += " JOIN FILE_LUMIS FLLU on FLLU.FILE_ID=FL.FILE_ID "
    		wheresql += " AND FLLU.RUN_NUM = :run_num "
    		binds.update(run_num = run_num)
    		sql += wheresql
            else:
                dbsExceptionHandler("dbsException-invalid-input", "Oracle/Dataset/List. Proper parameters are not\
                    provided for listDatasets call.")

        return self.api.query(match=None, select=None, sql=sql, **binds)
