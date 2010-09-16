#!/usr/bin/env python
"""
This module provides Migration.Update data access object.
"""
__revision__ = "$Id: Update.py,v 1.1 2010/06/29 19:28:46 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
"""UPDATE %sMIGRATION_BLOCKS
SET MIGRATION_STATUS=:migration_status 
WHERE MIGRATION_BLOCK=:migration_block""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_block
        """	
        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
