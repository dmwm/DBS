
#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: BriefList.py,v 1.2 2010/08/03 13:35:43 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

class BriefList(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	self.basesql = " D.DATASET FROM %sDATASETS D " %  self.owner

    def execute(self, conn, dataset="", is_dataset_valid=1, parent_dataset="",
                release_version="", pset_hash="", app_name="", output_module_label="", global_tag="",
                processing_version=0, acquisition_era="", run_num=-1,
                physics_group_name="", logical_file_name="", primary_ds_name="",
                primary_ds_type="", processed_ds_name="", data_tier_name="", dataset_access_type="", 
                prep_id="", create_by='', last_modified_by='', min_cdate=0, max_cdate=0, min_ldate=0, max_ldate=0, cdate=0,
                ldate=0, dataset_id=-1,
                transaction=False):
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Dataset/BriefList.  Expects db connection from upper layer.")
	selectsql = 'SELECT '
	joinsql = ''
        generatedsql = ''
        binds = {}
	wheresql = 'WHERE D.IS_DATASET_VALID=:is_dataset_valid '
        if dataset and type(dataset) is list:  # for the POST method
            #wheresql += " AND D.DATASET=:dataset "
            ds_generator, binds2 = create_token_generator(dataset)
	    binds.update(binds2)
            wheresql += " AND D.DATASET in (SELECT TOKEN FROM TOKEN_GENERATOR)"
            generatedsql = "{ds_generator}".format(ds_generator=ds_generator)
            if dataset_access_type and (dataset_access_type !="%" or dataset_access_type != '*'):
                joinsql += " JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID " % (self.owner)
                op = ("=", "like")["%" in dataset_access_type or "*" in dataset_access_type]
                wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type " %op
                binds['dataset_access_type'] = dataset_access_type
                binds['is_dataset_valid'] = is_dataset_valid
            else:
                binds['is_dataset_valid'] = is_dataset_valid
        elif dataset_id is not None and type(dataset_id) is not int:  # for the POST method
	    #we treat the datset_id is the same way as run_num. It can be id1-id2, id or [id1,2,3 ...]
	    dataset_id_list = []
	    wheresql_dataset_id_list=''
            wheresql_dataset_id_range=''
	    for id in parseRunRange(dataset_id):
		if isinstance(id, basestring) or isinstance(id, int) or isinstance(id, long):
		    dataset_id_list.append(str(id))
                if isinstance(id, run_tuple):
                    if id[0] == id[1]:
		        dbsExceptionHandler('dbsException-invalid-input', "DBS dataset_id range must be apart at least by 1.")
		    wheresql_dataset_id_range = " D.DATASET_ID between :minid and :maxid " 
                    binds.update({"minid":id[0]})
                    binds.update({"maxid":id[1]})
	    if dataset_id_list:
		ds_generator, binds2 = create_token_generator(dataset_id_list)
		binds.update(binds2)
		wheresql_dataset_id_list = " D.DATASET_ID in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		generatedsql = "{ds_generator}".format(ds_generator=ds_generator)
            if dataset_access_type and (dataset_access_type !="%" or dataset_access_type != '*'):
                joinsql += " JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID " %(self.owner)
                op = ("=", "like")["%" in dataset_access_type or "*" in dataset_access_type]
                wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type " %op
                binds['dataset_access_type'] = dataset_access_type
                binds['is_dataset_valid'] = is_dataset_valid
            else:
                binds['is_dataset_valid'] = is_dataset_valid
	    if wheresql_dataset_id_list and wheresql_dataset_id_range:
		wheresql += " and  (" + wheresql_dataset_id_list + " or " + wheresql_dataset_id_range + " )" 
	    elif wheresql_dataset_id_list and not wheresql_dataset_id_range:
		wheresql += " and " + wheresql_dataset_id_list
	    elif not wheresql_dataset_id_list and wheresql_dataset_id_range:	
		wheresql += " and " + wheresql_dataset_id_range	
        else: #for the GET method
            binds.update(is_dataset_valid=is_dataset_valid)
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
            elif min_ldate ==0 and max_ldate != 0:
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
                wheresql += "AND D.prep_id = :prep_id "
                binds.update(prep_id = prep_id)
            if dataset and isinstance(dataset, basestring) and dataset != "%":
                op = ("=", "like")["%" in dataset]
                wheresql += " AND D.DATASET %s :dataset " % op
                binds.update(dataset = dataset)
	    if dataset_id != -1:
                wheresql += " AND D.DATASET_ID = :dataset_id "
                binds.update(dataset_id = dataset_id) 	
            if primary_ds_name and primary_ds_name != "%":
                joinsql += " JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID " % (self.owner)
                op = ("=", "like")["%" in primary_ds_name ]
                wheresql += " AND P.PRIMARY_DS_NAME %s :primary_ds_name " % op
                binds.update(primary_ds_name = primary_ds_name)
            if primary_ds_type and  primary_ds_type !="%":
                if not primary_ds_name:  
                    joinsql += " JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID " % (self.owner)
                joinsql += " JOIN %sPRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID " % (self.owner)
                op = ("=", "like")["%" in primary_ds_type]
                wheresql += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type " %op
                binds.update(primary_ds_type=primary_ds_type)
            
            if processed_ds_name and processed_ds_name != "%":
                joinsql += " JOIN %sPROCESSED_DATASETS PR ON PR.PROCESSED_DS_ID = D.PROCESSED_DS_ID " % (self.owner)
                op = ("=", "like")["%" in processed_ds_name ]
                wheresql += " AND PR.PROCESSED_DS_NAME %s :processed_ds_name " % op
                binds.update(processed_ds_name = processed_ds_name)

            if data_tier_name and data_tier_name != "%":
                joinsql += " JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID " % (self.owner)
                op = ("=", "like")["%" in data_tier_name ]
                wheresql += " AND DT.DATA_TIER_NAME %s :data_tier_name " % op
                binds.update(data_tier_name=data_tier_name)

            if dataset_access_type and (dataset_access_type !="%" or dataset_access_type != '*'):
                joinsql += " JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID " % (self.owner)
                op = ("=", "like")["%" in dataset_access_type or "*" in dataset_access_type]
                wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type " %op
                binds.update(dataset_access_type=dataset_access_type)

            if physics_group_name and physics_group_name !="%":
                joinsql += " LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID " % (self.owner)
                op = ("=", "like")["%" in physics_group_name]
                wheresql += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name " %op
                binds.update(physics_group_name=physics_group_name)
   
            if parent_dataset:
                joinsql += """
                    LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
                    LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID
                    """ % ((self.owner,)*2)
                wheresql += " AND PDS.DATASET = :parent_dataset "
                binds.update(parent_dataset = parent_dataset)

            if release_version or pset_hash or app_name or output_module_label or global_tag:
                joinsql += """
                    LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID 
                    LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID  
                """ % ((self.owner,)*2)
	    
            if release_version:
                joinsql += " LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID " % (self.owner)
                op = ("=", "like")["%" in release_version]
                wheresql += " AND RV.RELEASE_VERSION %s :release_version " % op
                binds.update(release_version=release_version)

            if pset_hash:
                joinsql += " LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID " % (self.owner)
                op = ("=", "like")["%" in pset_hash]
                wheresql += " AND PSH.PSET_HASH %s :pset_hash " % op
                binds.update(pset_hash = pset_hash)

            if app_name:
                joinsql += " LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID " % (self.owner)
                op = ("=", "like")["%" in app_name]
                wheresql += " AND AEX.APP_NAME %s :app_name " % op
                binds.update(app_name = app_name)

            if output_module_label:
                op = ("=", "like")["%" in output_module_label]
                wheresql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label " % op
                binds.update(output_module_label=output_module_label)

            if global_tag:
                op = ("=", "like")["%" in global_tag]
                wheresql += " AND OMC.GLOBAL_TAG  %s :global_tag " % op
                binds.update(global_tag=global_tag)

            if processing_version != 0:
                joinsql += " LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID " % (self.owner)
                #op = ("=", "like")["%" in processing_version]
                op = "="
                wheresql += " AND PE.PROCESSING_VERSION %s :pversion " % op
                binds.update(pversion=processing_version)

            if acquisition_era:
                joinsql += " LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID " % (self.owner)
                op = ("=", "like")["%" in acquisition_era]
                wheresql += " AND AE.ACQUISITION_ERA_NAME %s :aera " % op
                binds.update(aera=acquisition_era)

            if logical_file_name and logical_file_name !="%":
                selectsql += "DISTINCT "
                joinsql += " JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID " % self.owner
                wheresql += " AND FL.LOGICAL_FILE_NAME = :logical_file_name "
                binds.update(logical_file_name = logical_file_name)
            #
            if  run_num != -1:
                if not logical_file_name:
                    selectsql += "DISTINCT "
                    joinsql += " JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID " % (self.owner)
                joinsql += " JOIN %sFILE_LUMIS FLLU on FLLU.FILE_ID=FL.FILE_ID " % (self.owner)
                run_list = []
                wheresql_run_list=''
                wheresql_run_range=''
                for r in parseRunRange(run_num):
                    if isinstance(r, basestring) or isinstance(r, int)  or isinstance(r, long):
                        run_list.append(str(r))
                    if isinstance(r, run_tuple):
                        if r[0] == r[1]:
                            dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.")
                        wheresql_run_range = " FLLU.RUN_NUM between :minrun and :maxrun "
                        binds.update({"minrun":r[0]})
                        binds.update({"maxrun":r[1]})
                # 
                if run_list:
                    wheresql_run_list = " FLLU.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
                    run_generator, run_binds = create_token_generator(run_list)
                    generatedsql =  "{run_generator}".format(run_generator=run_generator) 
                    binds.update(run_binds)
                if wheresql_run_range and wheresql_run_list:
                    wheresql += " and ("   + wheresql_run_range + " or " +  wheresql_run_list + " )"
                elif wheresql_run_range and not wheresql_run_list:
                    wheresql += " and "  + wheresql_run_range
                elif not wheresql_run_range and wheresql_run_list:
                    wheresql += " and " + wheresql_run_list

	sql = "".join((generatedsql, selectsql, self.basesql, joinsql, wheresql)) 
	#self.logger.error( sql)
        #self.logger.error( binds)
        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for i in cursors:
            d = self.formatCursor(i)
            if d:
                result += d
        return result
