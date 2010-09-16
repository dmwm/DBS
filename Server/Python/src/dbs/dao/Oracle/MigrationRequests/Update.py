#!/usr/bin/env python
"""
This module provides Migration.Update data access object.
"""
__revision__ = "$Id: Update.py,v 1.2 2010/06/28 21:29:24 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
class Update(DBFormatter):
    """
    Migration Update DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""UPDATE %sMIGRATION_REQUESTS 
SET MIGRATION_STATUS=:migration_status 
WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_request_id
        """	
        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
