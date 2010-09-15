#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2009/10/21 21:24:13 afaq Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
		SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID,D.PRIMARY_DS_ID, D.PROCESSED_DS_ID, D.DATA_TIER_ID,
		    D.DATASET_TYPE_ID, D.ACQUISITION_ERA_ID, D.PROCESSING_ERA_ID, D.PHYSICS_GROUP_ID, D.XTCROSSSECTION, D.GLOBAL_TAG,
		    D.CREATION_DATE, D.CREATE_BY, D.LAST_MODIFICATION_DATE,D.LAST_MODIFIED_BY,  PR.PRIMARY_DS_NAME,
		    DT.DATA_TIER_NAME, PS.PROCESSED_DS_NAME, PH.PHYSICS_GROUP_NAME, DP.DATASET_TYPE
                    FROM %sDATASETS D 
                    JOIN %sPRIMARY_DATASETS PR ON  PR.PRIMARY_DS_ID = D.PRIMARY_DS_ID 
		    JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
		    JOIN %sPROCESSED_DATASETS PS ON PS.PROCESSED_DS_ID = D.PROCESSED_DS_ID  
		    JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
		    JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
""" % ( self.owner, self.owner, self.owner, self.owner, self.owner, self.owner)
	
    def executeNONE(self, pattern = "", conn = None, transaction = False):
        """
        Lists all datasets if pattern is not provided.

	WE can add the use cases here, when:
		-- primary dataset name is provided
		-- dataset name is provided
		-- may be some other criteria is provided (?)
        """	
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            if  pattern.find("%")==-1:
                sql += "WHERE P.PRIMARY_DS_NAME = :name"
            else:
                sql += "WHERE P.PRIMARY_DS_NAME like  :primdsname"
            binds = {"primdsname":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)

