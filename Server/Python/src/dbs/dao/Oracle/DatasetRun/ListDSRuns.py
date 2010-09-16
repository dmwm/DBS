#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: ListDSRuns.py,v 1.6 2010/03/18 18:53:42 afaq Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListDSRuns(DBFormatter):
    """
    DatasetRun ListDSRuns DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT DISTINCT FL.RUN_NUM, DS.DATASET
	FROM %sFILE_LUMIS FL
	JOIN %sFILES F ON F.FILE_ID = FL.FILE_ID 
	JOIN %sDATASETS DS ON F.DATASET_ID = DS.DATASET_ID
	WHERE DATASET = :dataset """% ((self.owner,) * 3)
	
    def execute(self, conn, dataset="", minrun=-1, maxrun=-1, trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/DatasetRun/ListDSRuns expects db connection from upper layer.")
        sql = self.sql
        binds = {'dataset':dataset}
	if minrun > 0: 
		sql += " AND FL.RUN_NUM >= :min_run"
		binds["min_run"] = minrun
	if maxrun > 0:
		if minrun > 0:
		    sql += " AND FL.RUN_NUM <= :max_run"
		else:
		     sql += " where FL.RUN_NUM <= :max_run"
		binds["max_run"] = maxrun

	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
