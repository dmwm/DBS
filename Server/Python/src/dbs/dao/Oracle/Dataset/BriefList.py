
#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: BriefList.py,v 1.2 2010/08/03 13:35:43 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class BriefList(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        For now IS_DATASET_VALID = 1 
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	self.basesql = " D.DATASET FROM %sDATASETS D " %  self.owner
	self.wheresql = " WHERE D.IS_DATASET_VALID = 1"

    def execute(self, conn, dataset="", parent_dataset="",
                release_version="", pset_hash="", app_name="", output_module_label="",
                processing_version="", acquisition_era="", run_num=0,
                physics_group_name="", logical_file_name="", primary_ds_name="",
                primary_ds_type="", data_tier_name="", dataset_access_type="", transaction=False):

        if not conn:
            raise Exception("dbs/dao/Oracle/Dataset/List expects db connection from upper layer.")

	selectsql = 'SELECT '
	joinsql = ''
	wheresql = self.wheresql
        binds = {}
	
        if dataset and dataset != "%":
	    op = ("=", "like")["%" in dataset]
	    wheresql += " AND D.DATASET %s :dataset" % op
	    binds.update(dataset = dataset)

        if primary_ds_name and primary_ds_name != "%":
	    joinsql += " JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID" % (self.owner)
	    op = ("=", "like")["%" in primary_ds_name ]
	    wheresql += " AND P.PRIMARY_DS_NAME %s :primary_ds_name" % op
	    binds.update(primary_ds_name = primary_ds_name)

        if primary_ds_type and  primary_ds_type !="%":
	    if not primary_ds_name:  
		joinsql += " JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID" % (self.owner)
	    joinsql += " JOIN %sPRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID" % (self.owner)
	    op = ("=", "like")["%" in primary_ds_type]
	    wheresql += " AND PDT.PRIMARY_DS_TYPE %s :primary_ds_type" %op
	    binds.update(primary_ds_type=primary_ds_type)

        if data_tier_name and data_tier_name != "%":
	    joinsql += " JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID" % (self.owner)
	    op = ("=", "like")["%" in data_tier_name ]
	    wheresql += " AND DT.DATA_TIER_NAME %s :data_tier_name" % op
	    binds.update(data_tier_name=data_tier_name)

        if dataset_access_type and dataset_access_type !="%":
	    joinsql += " JOIN %sDATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID" % (self.owner)
	    op = ("=", "like")["%" in dataset_access_type]
	    wheresql += " AND DP.DATASET_ACCESS_TYPE %s :dataset_access_type" %op
	    binds.update(dataset_access_type=dataset_access_type)

        if physics_group_name and physics_group_name !="%":
	    joinsql += " LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID" % (self.owner)
	    op = ("=", "like")["%" in physics_group_name]
	    wheresql += " AND PH.PHYSICS_GROUP_NAME %s :physics_group_name" %op
	    binds.update(physics_group_name=physics_group_name)
   
        if parent_dataset:
	    joinsql += """
		LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
		LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID
		""" % ((self.owner,)*2)
	    wheresql += " AND PDS.DATASET = :parent_dataset"
	    binds.update(parent_dataset = parent_dataset)

	if release_version or pset_hash or app_name or output_module_label:
	    joinsql += """
		LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
		LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID 
	    """ % ((self.owner,)*2)
	    
	if release_version:
	    joinsql += " LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID" % (self.owner)
	    op = ("=", "like")["%" in release_version]
	    wheresql += " AND RV.RELEASE_VERSION %s :release_version" % op
	    binds.update(release_version=release_version)

        if pset_hash:
	    joinsql += " LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID" % (self.owner)
	    op = ("=", "like")["%" in pset_hash]
	    wheresql += " AND PSH.PSET_HASH %s :pset_hash" % op
	    binds.update(pset_hash = pset_hash)

        if app_name:
	    joinsql += " LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID" % (self.owner)
	    op = ("=", "like")["%" in app_name]
	    wheresql += " AND AEX.APP_NAME %s :app_name" % op
	    binds.update(app_name = app_name)

        if output_module_label:
	    op = ("=", "like")["%" in output_module_label]
	    wheresql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" % op
	    binds.update(output_module_label=output_module_label)

        if processing_version:
	    joinsql += " LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID" % (self.owner)
	    op = ("=", "like")["%" in processing_version]
	    wheresql += " AND PE.PROCESSING_VERSION %s :pversion" % op
	    binds.update(pversion=processing_version)

        if acquisition_era:
	    joinsql += " LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID" % (self.owner)
	    op = ("=", "like")["%" in acquisition_era]
	    wheresql += " AND AE.ACQUISITION_ERA_NAME %s :aera" % op
	    binds.update(aera=acquisition_era)

	if logical_file_name and logical_file_name !="%":
	    selectsql += "DISTINCT "
	    joinsql += " JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID" % self.owner
	    wheresql += " AND FL.LOGICAL_FILE_NAME = :logical_file_name"
	    binds.update(logical_file_name = logical_file_name)

	if  run_num and run_num!=0:
	    if not logical_file_name:
		selectsql += "DISTINCT "
		joinsql += " JOIN %sFILES FL on FL.DATASET_ID = D.DATASET_ID" % (self.owner)
	    joinsql += " JOIN %sFILE_LUMIS FLLU on FLLU.FILE_ID=FL.FILE_ID" % (self.owner)
	    wheresql += " AND FLLU.RUN_NUM = :run_num"
	    binds.update(run_num = run_num)

	sql = "".join((selectsql, self.basesql, joinsql, wheresql)) 
	

	#print "sql=%s" %sql
	#print "binds=%s" %binds
        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        assert len(cursors) == 1, "block does not exist"
        result = self.formatCursor(cursors[0])

        return result

