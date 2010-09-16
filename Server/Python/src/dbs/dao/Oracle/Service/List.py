#!/usr/bin/env python
"""
This module provides Services.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/08/02 20:41:18 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    Services List DAO class.
    """

    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
	
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """
	SELECT SERVICE_ID, NAME, TYPE, LOCATION, 
	    STATUS, ADMIN, URI, DB, VERSION, 
	    LAST_CONTACT, ALIAS, COMMENTS 
	    FROM %sSERVICES """ % (self.owner)

    def execute(self, conn, transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
	    raise Exception("dbs/dao/Oracle/Services/List expects db connection from upper layer.")	    
        sql = self.sql
        binds = {}
        
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
