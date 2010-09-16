#!/usr/bin/env python
"""
This module provides Site.ListBlockSite data access object.
"""
__revision__ = "$Id: ListBlockSite.py,v 1.1 2010/04/21 19:50:02 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
	SELECT SE.SITE_NAME, B.BLOCK_NAME 
	FROM %sSITES SE
		JOIN %sBLOCK_SITES BSE
			ON BSE.SITE_ID = SE.SITE_ID
		JOIN %sBLOCKS B
			ON B.BLOCK_ID = BSE.BLOCK_ID
	WHERE BLOCK_NAME = :block_name""" % ((self.owner,) * 3)

    def execute(self, conn, block_name = "", trans = False):
        """
        Lists all storage elements for the block.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/Site/ListBlockSite expects db connection from up layer.")
	sql = self.sql

	binds={ "block_name" : block_name }
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
	return result


