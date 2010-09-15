#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: List1.py,v 1.5 2010/01/25 23:20:56 afaq Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class List1(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        For now IS_DATASET_VALID = 1 and
        DP.DATASET_TYPE <> 'DELETED' are hardcoded in self.sql.
        We might need to pass these parameters from outside later
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = ("", "%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
        P.PRIMARY_DS_NAME,
        PD.PROCESSED_DS_NAME,
        DT.DATA_TIER_NAME,
        DP.DATASET_TYPE,
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
JOIN %sPROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID

JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID

LEFT OUTER JOIN %sDATASET_PARENTS DSP ON DSP.THIS_DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sDATASETS PDS ON PDS.DATASET_ID = DSP.PARENT_DATASET_ID

LEFT OUTER JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID = D.DATASET_ID
LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID

WHERE D.IS_DATASET_VALID = 1
AND DP.DATASET_TYPE <> 'DELETED'
""" % ((self.owner,)*15)

    def execute(self, dataset="", parent_dataset="", 
                release_version="", pset_hash="", app_name="", output_module_label="", 
                conn=None):
        """
        dataset key is a wild card parameter
        """	
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        binds = {}
        
        if dataset:
            op = ("=", "like")["%" in dataset]
            sql += " AND D.DATASET %s :dataset" % op 
            binds.update(dataset = dataset)
        if parent_dataset:
            sql += " AND PDS.DATASET = :parent_dataset"
            binds.update(parent_dataset = parent_dataset)
        if release_version:
            sql += " AND RV.RELEASE_VERSION = :release_version"
            binds.update(release_version=release_version)
        if pset_hash:
            sql += " AND PSH.PSET_HASH = :pset_hash"
            binds.update(pset_hash = pset_hash)
        if app_name:
            sql += " AND AEX.APP_NAME = :app_name"
            binds.update(app_name = app_name)
        if output_module_label:
            sql += " AND OMC.OUTPUT_MODULE_LABEL  = :output_module_label" 
            binds.update(output_module_label=output_module_label)

        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
