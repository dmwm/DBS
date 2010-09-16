#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: List.py,v 1.31 2010/05/19 16:19:21 yuyi Exp $"
__version__ = "$Revision: 1.31 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
	"""
        Add schema owner and sql.
        For now IS_DATASET_VALID = 1 and
        DP.DATASET_ACCESS_TYPE <> 'DELETED' are hardcoded in self.sql.
        We might need to pass these parameters from outside later
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	self.sql = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
        P.PRIMARY_DS_NAME,
	PDT.PRIMARY_DS_TYPE,
        PD.PROCESSED_DS_NAME,
        DT.DATA_TIER_NAME,
        DP.DATASET_ACCESS_TYPE,
        AE.ACQUISITION_ERA_NAME,
        PE.PROCESSING_VERSION,
        PH.PHYSICS_GROUP_NAME, 
        PDS.DATASET parent_dataset,
        OMC.OUTPUT_MODULE_LABEL,
        RV.RELEASE_VERSION,
        PSH.PSET_HASH,
        AEX.APP_NAME
        
FROM %sDATASETS D
JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
JOIN %sPRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
JOIN %sPROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID

LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID

LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID

WHERE D.IS_DATASET_VALID = 1
AND DP.DATASET_ACCESS_TYPE <> 'DELETED'
""" % ((self.owner,)*16)
#
	self.sql1 = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
        P.PRIMARY_DS_NAME,
	PDT.PRIMARY_DS_TYPE,
        PD.PROCESSED_DS_NAME,
        DT.DATA_TIER_NAME,
        DP.DATASET_ACCESS_TYPE,
        AE.ACQUISITION_ERA_NAME,
        PE.PROCESSING_VERSION,
        PH.PHYSICS_GROUP_NAME, 
        PDS.DATASET parent_dataset,
        OMC.OUTPUT_MODULE_LABEL,
        RV.RELEASE_VERSION,
        PSH.PSET_HASH,
        AEX.APP_NAME
        
FROM %sDATASETS D
JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
JOIN %sPRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
JOIN %sPROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID 
LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID

LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID

LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID

WHERE D.IS_DATASET_VALID = 1
AND DP.DATASET_ACCESS_TYPE <> 'DELETED'
""" % ((self.owner,)*17)
#
	self.sql2 = \
"""
SELECT DISTINCT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
        P.PRIMARY_DS_NAME,
	PDT.PRIMARY_DS_TYPE,
        PD.PROCESSED_DS_NAME,
        DT.DATA_TIER_NAME,
        DP.DATASET_ACCESS_TYPE,
        AE.ACQUISITION_ERA_NAME,
        PE.PROCESSING_VERSION,
        PH.PHYSICS_GROUP_NAME, 
        PDS.DATASET parent_dataset,
        OMC.OUTPUT_MODULE_LABEL,
        RV.RELEASE_VERSION,
        PSH.PSET_HASH,
        AEX.APP_NAME
        
FROM %sDATASETS D
JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
JOIN %sPRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
JOIN %sPROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID 
JOIN %sFILE_LUMIS FLLU on FLLU.FILE_ID=FL.FILE_ID 
LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID

LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID

LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID

WHERE D.IS_DATASET_VALID = 1
AND DP.DATASET_ACCESS_TYPE <> 'DELETED'
""" % ((self.owner,)*18)
#    
    def execute1(self, conn, dataset="", parent_dataset="", 
                release_version="", pset_hash="", app_name="", output_module_label="", 
                processing_version="", acquisition_era="", 
		physics_group_name="", logical_file_name="", primary_ds_name="",
                primary_ds_type="", data_tier_name="", dataset_access_type="", transaction=False):
	"""
        dataset key is a wild card parameter
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/List expects db connection from up layer.") 
            
	sql1 = self.sql1
        binds = {}
        
	if dataset and dataset != "%":
	    op = ("=", "like")["%" in dataset]
            sql1 += " AND D.DATASET %s :dataset" % op 
            binds.update(dataset = dataset)
	if primary_ds_name and primary_ds_name != "%":
	   op = ("=", "like")["%" in primary_ds_name ]
	   sql1 += " AND P.PRIMARY_DS_NAME %s :primary_ds_name" % op
	   binds.update(primary_ds_name = primary_ds_name)
	if data_tier_name and data_tier_name != "%":
	   op = ("=", "like")["%" in data_tier_name ]
	   sql1 += " AND DT.DATA_TIER_NAME %s :data_tier_name" % op
	   binds.update(data_tier_name=data_tier_name)
	if dataset_access_type and dataset_access_type !="%":
	   op = ("=", "like")["%" in dataset_access_type]
	   sql1 += " AND DP.DATASET_ACCESS_TYPE %s : dataset_access_type" %op
	   binds.update(dataset_access_type=dataset_access_type)
	if primary_ds_type and  primary_ds_type !="%":
	   op = ("=", "like")["%" in primary_ds_type]
	   sql1 += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type" %op
	   binds.update(primary_ds_type=primary_ds_type)
	if physics_group_name and physics_group_name !="%":
	   op = ("=", "like")["%" in physics_group_name]
	   sql1 += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name" %op
	   binds.update(physics_group_name=physics_group_name)
	if logical_file_name and logical_file_name !="%": 
           #op = ("=", "like")["%" in logical_file_name] 
           sql1 += " AND FL.LOGICAL_FILE_NAME = :logical_file_name"
           binds.update(logical_file_name=logical_file_name)	
        if parent_dataset:
            sql1 += " AND PDS.DATASET = :parent_dataset"
            binds.update(parent_dataset = parent_dataset)
        if release_version:
	    op = ("=", "like")["%" in release_version]
            sql1 += " AND RV.RELEASE_VERSION %s :release_version" % op
            binds.update(release_version=release_version)
        if pset_hash:
	    op = ("=", "like")["%" in pset_hash]
            sql1 += " AND PSH.PSET_HASH %s :pset_hash" % op
            binds.update(pset_hash = pset_hash)
        if app_name:
	    op = ("=", "like")["%" in app_name]
            sql1 += " AND AEX.APP_NAME %s :app_name" % op
            binds.update(app_name = app_name)
        if output_module_label:
	    op = ("=", "like")["%" in output_module_label]
            sql1 += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" % op
            binds.update(output_module_label=output_module_label)
	if processing_version:
	    op = ("=", "like")["%" in processing_version]
	    sql1 += " AND PE.PROCESSING_VERSION %s :pversion" % op
	    binds.update(pversion=processing_version)
	if acquisition_era:
	    op = ("=", "like")["%" in acquisition_era]
	    sql1 += " AND AE.ACQUISITION_ERA_NAME %s :aera" % op
	    binds.update(aera=acquisition_era)
	#print "sql=%s" %sql1
	#print "binds=%s" %binds
	#self.logger.info(binds)
	#sql1 = "select 1 "
	#binds = {}
	cursors = self.dbi.processData(sql1, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "dataset does not exist"
	result = self.formatCursor(cursors[0])
	
	#print len(cursors)
	#for c in cursors:
	#	print c.fetchall()
	#result = self.format(cursors)
	#print "result=%s" %result
	return result
	 
    def execute2(self, conn, dataset="", parent_dataset="", 
                release_version="", pset_hash="", app_name="", output_module_label="", 
                processing_version="", acquisition_era="", run_num=0, 
		physics_group_name="", logical_file_name="", primary_ds_name="",
                primary_ds_type="", data_tier_name="", dataset_access_type="", transaction=False):
	"""
        dataset key is a wild card parameter
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/List expects db connection from up layer.") 
            
	sql2 = self.sql2
	binds = {}
        
	if dataset and dataset != "%":
	    op = ("=", "like")["%" in dataset]
            sql2 += " AND D.DATASET %s :dataset" % op 
            binds.update(dataset = dataset)
	if logical_file_name:
	   sql2 += " AND FL.LOGICAL_FILE_NAME = :logical_file_name"
	   binds.update(logical_file_name = logical_file_name)  
	if run_num and run_num != 0:
	   sql2 += " AND FLLU.RUN_NUM = :run_num"
	   binds.update(run_num = run_num)
	if primary_ds_name and primary_ds_name != "%":
	   op = ("=", "like")["%" in primary_ds_name ]
	   sql2 += " AND P.PRIMARY_DS_NAME %s :primary_ds_name" % op
	   binds.update(primary_ds_name = primary_ds_name)
	if data_tier_name and data_tier_name != "%":
	   op = ("=", "like")["%" in data_tier_name ]
	   sql2 += " AND DT.DATA_TIER_NAME %s :data_tier_name" % op
	   binds.update(data_tier_name=data_tier_name)
	if dataset_access_type and dataset_access_type !="%":
	   op = ("=", "like")["%" in dataset_access_type]
	   sql2 += " AND DP.DATASET_ACCESS_TYPE %s : dataset_access_type" %op
	   binds.update(dataset_access_type=dataset_access_type)
	if primary_ds_type and  primary_ds_type !="%":
	   op = ("=", "like")["%" in primary_ds_type]
	   sql2 += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type" %op
	   binds.update(primary_ds_type=primary_ds_type)
	if physics_group_name and physics_group_name !="%":
	   op = ("=", "like")["%" in physics_group_name]
	   sql2 += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name" %op
	   binds.update(physics_group_name=physics_group_name)	
        if parent_dataset:
            sql2 += " AND PDS.DATASET = :parent_dataset"
            binds.update(parent_dataset = parent_dataset)
        if release_version:
	    op = ("=", "like")["%" in release_version]
            sql2 += " AND RV.RELEASE_VERSION %s :release_version" % op
            binds.update(release_version=release_version)
        if pset_hash:
	    op = ("=", "like")["%" in pset_hash]
            sql2 += " AND PSH.PSET_HASH %s :pset_hash" % op
            binds.update(pset_hash = pset_hash)
        if app_name:
	    op = ("=", "like")["%" in app_name]
            sql2 += " AND AEX.APP_NAME %s :app_name" % op
            binds.update(app_name = app_name)
        if output_module_label:
	    op = ("=", "like")["%" in output_module_label]
            sql2 += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" % op
            binds.update(output_module_label=output_module_label)
	if processing_version:
	    op = ("=", "like")["%" in processing_version]
	    sql2 += " AND PE.PROCESSING_VERSION %s :pversion" % op
	    binds.update(pversion=processing_version)
	if acquisition_era:
	    op = ("=", "like")["%" in acquisition_era]
	    sql2 += " AND AE.ACQUISITION_ERA_NAME %s :aera" % op
	    binds.update(aera=acquisition_era)
	#print "sql2=%s" %sql2
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql2, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
	result = self.formatCursor(cursors[0])

	return result
	 
    def execute(self, conn, dataset="", parent_dataset="", 
                release_version="", pset_hash="", app_name="", output_module_label="", 
                processing_version="", acquisition_era="", 
		physics_group_name="", primary_ds_name="",
                primary_ds_type="", data_tier_name="", dataset_access_type="", transaction=False):
        """
        dataset key is a wild card parameter
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/List expects db connection from up layer.") 
            
        sql = self.sql
        binds = {}
        
	if dataset and dataset != "%":
            op = ("=", "like")["%" in dataset]
            sql += " AND D.DATASET %s :dataset" % op 
            binds.update(dataset = dataset)
	if primary_ds_name and primary_ds_name != "%":
	   op = ("=", "like")["%" in primary_ds_name ]
	   sql += " AND P.PRIMARY_DS_NAME %s :primary_ds_name" % op
	   binds.update(primary_ds_name = primary_ds_name)
	if data_tier_name and data_tier_name != "%":
	   op = ("=", "like")["%" in data_tier_name ]
	   sql += " AND DT.DATA_TIER_NAME %s :data_tier_name" % op
	   binds.update(data_tier_name=data_tier_name)
	if dataset_access_type and dataset_access_type !="%":
	   op = ("=", "like")["%" in dataset_access_type]
	   sql += " AND DP.DATASET_ACCESS_TYPE %s : dataset_access_type" %op
	   binds.update(dataset_access_type=dataset_access_type)
	if primary_ds_type and  primary_ds_type !="%":
	   op = ("=", "like")["%" in primary_ds_type]
	   sql += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type" %op
	   binds.update(primary_ds_type=primary_ds_type)
	if physics_group_name and physics_group_name !="%":
	   op = ("=", "like")["%" in physics_group_name]
	   sql += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name" %op
	   binds.update(physics_group_name=physics_group_name)	
        if parent_dataset:
            sql += " AND PDS.DATASET = :parent_dataset"
            binds.update(parent_dataset = parent_dataset)
        if release_version:
	    op = ("=", "like")["%" in release_version]
            sql += " AND RV.RELEASE_VERSION %s :release_version" % op
            binds.update(release_version=release_version)
        if pset_hash:
	    op = ("=", "like")["%" in pset_hash]
            sql += " AND PSH.PSET_HASH %s :pset_hash" % op
            binds.update(pset_hash = pset_hash)
        if app_name:
	    op = ("=", "like")["%" in app_name]
            sql += " AND AEX.APP_NAME %s :app_name" % op
            binds.update(app_name = app_name)
        if output_module_label:
	    op = ("=", "like")["%" in output_module_label]
            sql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" % op
            binds.update(output_module_label=output_module_label)
	if processing_version:
	    op = ("=", "like")["%" in processing_version]
	    sql += " AND PE.PROCESSING_VERSION %s :pversion" % op
	    binds.update(pversion=processing_version)
	if acquisition_era:
	    op = ("=", "like")["%" in acquisition_era]
	    sql += " AND AE.ACQUISITION_ERA_NAME %s :aera" % op
	    binds.update(aera=acquisition_era)
	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
	result = self.formatCursor(cursors[0])

	return result
	
