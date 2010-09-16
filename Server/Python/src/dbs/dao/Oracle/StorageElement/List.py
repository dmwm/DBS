#!/usr/bin/env python
"""
This module provides StorageElement.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/03/02 20:17:19 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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

    def execute(self, se_name = "", conn = None, trans = False):
        """
        Lists all storage elements.
	se_name can be a pattern as well
        """

	if not conn:
		raise "database conection does not exist"
	sql = self.sql
	
	if se_name:
		op = ("=", "like")["%" in se_name]
		sql += " WHERE SE.SE_NAME %s :se_name" % op
		binds={ "se_name" : se_name }

	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
	return result






