#!/usr/bin/env python
"""
This module provides DatasetParent.List data access object.
"""
__revision__ = "$Id: List.py,v 1.6 2010/08/12 18:39:11 yuyi Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    DatasetParent List DAO class.
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
SELECT PD.DATASET parent_dataset, 
       PD.DATASET_ID parent_dataset_id,
       D.DATASET this_dataset
FROM %sDATASETS PD
JOIN %sDATASET_PARENTS DP ON DP.PARENT_DATASET_ID = PD.DATASET_ID
JOIN %sDATASETS D ON  D.DATASET_ID = DP.THIS_DATASET_ID 
""" % ((self.owner,)*3)

    def execute(self, conn, dataset, transaction=False):
        """ dataset is required parameter"""
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/DatasetParent/List. Expects db connection from upper layer.", self.logger.exception)
            
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":dataset}
	#self.logger.error( sql)
        #self.logger.error("binds=%s" %binds)
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	#assert len(cursors) == 1, "Dataset parent does not exist"
        result = []
        for c in cursors:
            result.extend(self.formatCursor(c))
        return result    
        #conn.close()
        return result
