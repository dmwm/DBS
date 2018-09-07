#!/usr/bin/env python
"""
This module provides DatasetParent.ListChild data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter

class ListChild(DBFormatter):
    """
    DatasetParent ListChild DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = \
"""
SELECT CD.DATASET child_dataset, 
       CD.DATASET_ID child_dataset_id,
       D.DATASET
FROM %sDATASETS CD
JOIN %sDATASET_PARENTS DC ON DC.THIS_DATASET_ID = CD.DATASET_ID
JOIN %sDATASETS D ON  D.DATASET_ID = DC.PARENT_DATASET_ID 
""" % ((self.owner,)*3)

    def execute(self, conn, dataset, transaction=False):
        """ dataset is required parameter"""
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/DatasetParent/ListChild. Expects db connection from upper layer.", self.logger.exception)
        
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":dataset}
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	#assert len(cursors) == 1, "Dataset parent does not exist"
        result = []
        for c in cursors:
            result.extend(self.formatCursor(c, size=100))
        return result
        conn.close()
        return result
