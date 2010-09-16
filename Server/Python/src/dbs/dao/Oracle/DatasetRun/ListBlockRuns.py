#!/usr/bin/env python
"""
This module provides DatasetRun.ListBlockRuns data access object.
"""
__revision__ = "$Id: ListBlockRuns.py,v 1.3 2010/03/05 16:51:49 yuyi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListBlockRuns(DBFormatter):
    """
    DatasetRun ListBlockRuns DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT DISTINCT DR.RUN_NUMBER, B.BLOCK_NAME
	FROM %sDATASET_RUNS DR
	JOIN %sDATASET DS ON DR.DATASET_ID = DS.DATASET_ID
	JOIN %sBLOCK B ON B.DATASET_ID = DS.DATASET_ID
	WHERE B.BLOCK_NAME = :block_name"""% ((self.owner,) *3 )
	
    def execute(self, conn, block_name="", minRun=-1, maxRun=-1, trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/DatasetRun/ListBlockRuns expects db connection from up layer.")
        sql = self.sql
        binds = { "block_name" : block_name }
	if minRun > 0: 
		sql += " AND DR.RUN_NUMBER >= :min_run"
		binds["min_run"] = minRun
	if maxRun > 0:
		sql += " AND DR.RUN_NUMBER <= :max_run"
		binds["max_run"] = maxRun

	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
