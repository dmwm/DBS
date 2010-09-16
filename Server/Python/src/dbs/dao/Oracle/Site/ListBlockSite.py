#!/usr/bin/env python
"""
This module provides Site.ListBlockSite data access object.
"""
__revision__ = "$Id: ListBlockSite.py,v 1.3 2010/06/23 21:21:26 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

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

	if not conn:
		raise Exception("dbs/dao/Oracle/Site/ListBlockSite expects db connection from upper layer.")
	sql = self.sql

	binds={ "block_name" : block_name }
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
	return result


