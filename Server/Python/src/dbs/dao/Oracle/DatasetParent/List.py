#!/usr/bin/env python
"""
This module provides DatasetParent.List data access object.
"""
__revision__ = "$Id: List.py,v 1.3 2010/02/18 20:00:39 yuyi Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    DatasetParent List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = \
"""
SELECT PD.DATASET parent_dataset, 
       PD.DATASET_ID parent_dataset_id,
       D.DATASET
FROM %sDATASETS PD
JOIN %sDATASET_PARENTS DP ON DP.PARENT_DATASET_ID = PD.DATASET_ID
JOIN %sDATASETS D ON  D.DATASET_ID = DP.THIS_DATASET_ID 
""" % ((self.owner,)*3)

    def execute(self, dataset, conn=None):
        """ dataset is required parameter"""
        if not conn:
            conn = self.dbi.connection()
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":dataset}
	cursors = self.dbi.processData(sql, binds, conn, transaction=False, returnCursor=True)
	assert len(cursors) == 1, "Dataset parent does not exist"
        result = self.formatCursor(cursors[0])
        conn.close()
        return result
