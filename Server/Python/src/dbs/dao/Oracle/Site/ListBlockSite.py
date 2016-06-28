#!/usr/bin/env python
"""
This module provides Site.ListBlockSite data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class ListBlockSite(DBFormatter):
    """
    StorageElement List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT ORIGIN_SITE_NAME, B.BLOCK_NAME 
	FROM %sBLOCKS 
	WHERE BLOCK_NAME = :block_name""" % self.owner

    def execute(self, conn, block_name = "", trans = False):
        """
        Lists all sites for the block.
        """
	sql = self.sql

	binds={ "block_name" : block_name }
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = []
        for c in cursors:
            result.extend(self.formatCursor(c))
	return result


