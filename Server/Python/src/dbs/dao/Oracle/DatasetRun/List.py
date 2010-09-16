#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.8 2010/06/23 21:21:22 afaq Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    DatasetRun List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT DISTINCT FL.RUN_NUM
	FROM %sFILE_LUMIS FL"""% (self.owner)
	
    def execute(self, conn, minrun=-1, maxrun=-1, trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/DatasetRun/List expects db connection from upper layer.")	
        sql = self.sql
        binds = {}
	if minrun > 0: 
		sql += " where FL.RUN_NUM >= :min_run"
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
