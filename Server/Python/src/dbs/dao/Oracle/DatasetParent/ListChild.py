#!/usr/bin/env python
"""
This module provides DatasetParent.ListChild data access object.
"""
__revision__ = "$Id: ListChild.py,v 1.4 2010/06/23 21:21:22 afaq Exp $"
__version__ = "$Revision: 1.4 $"

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
            raise Exception("dbs/dao/Oracle/DatasetParent/List expects db connection from upper layer.")
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":dataset}
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "Dataset parent does not exist"
        result = self.formatCursor(cursors[0])
        conn.close()
        return result
