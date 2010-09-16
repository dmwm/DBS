#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/02/26 18:48:25 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
	SELECT DISTINCT DR.RUN_NUMBER
	FROM %sDATASET_RUNS"""% (self.owner)
	
    def execute(self, primary_ds_name="", conn=None, trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
	
        sql = self.sql
        binds = {}
	cursors = self.dbi.processData(sql, binds, conn, transaction=False, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
