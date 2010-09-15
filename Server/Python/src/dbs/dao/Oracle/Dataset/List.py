#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.16 2010/01/01 18:56:21 akhukhun Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
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
        PH.PHYSICS_GROUP_NAME
FROM %sDATASETS D
JOIN %sPRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
JOIN %sPROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
LEFT OUTER JOIN %sACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
LEFT OUTER JOIN %sPROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
WHERE D.IS_DATASET_VALID = 1
AND DP.DATASET_TYPE <> 'DELETED'
""" % ((self.owner,)*8)

    def execute(self, dataset="", conn=None):
        """
        dataset is a wild card parameter and can include % character
        """	
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        cursor = conn.connection.cursor()
        
        if not dataset:
            cursor.execute(sql)
        else:
            op = ("=", "like")["%" in dataset]
            sql += " AND D.DATASET %s :dataset" % op 
            binds = {"dataset":dataset}
            cursor.execute(sql, binds)
            
        result = self.formatCursor(cursor)
        conn.close()
        return result
