#!/usr/bin/env python
"""
This module provides StorageElement.List data access object.
"""
__revision__ = "$Id: List.py,v 1.3 2010/06/23 21:21:26 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
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
	SELECT SE.SE_NAME 
	FROM %sSTORAGE_ELEMENTS SE""" % (self.owner)

    def execute(self, conn, se_name = "", trans = False):
        """
        Lists all storage elements.
	se_name can be a pattern as well
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/StorageElement/List expects db connection from upper layer.")
	sql = self.sql
	
	if se_name:
		op = ("=", "like")["%" in se_name]
		sql += " WHERE SE.SE_NAME %s :se_name" % op
		binds={ "se_name" : se_name }

	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
	return result






