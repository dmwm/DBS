#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2010/03/18 17:13:02 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
	
    def execute(self, conn, minRun=-1, maxRun=-1, trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/DatasetRun/List expects db connection from up layer.")	
        sql = self.sql
        binds = {}
	if minRun > 0: 
		sql += " where FL.RUN_NUM >= :min_run"
		binds["min_run"] = minRun
	if maxRun > 0:
		if minRun > 0:
			sql += " AND FL.RUN_NUM <= :max_run"
		else:
			sql += " where FL.RUN_NUM <= :max_run"
		binds["max_run"] = maxRun
	print sql, binds
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
