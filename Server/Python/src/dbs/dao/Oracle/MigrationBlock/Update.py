#!/usr/bin/env python
"""
This module provides Migration.Update data access object.
"""
__revision__ = "$Id: Update.py,v 1.2 2010/07/09 14:41:00 afaq Exp $"
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
"""UPDATE %sMIGRATION_BLOCKS
SET MIGRATION_STATUS=:migration_status 
WHERE MIGRATION_BLOCK_NAME=:migration_block_name""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_block
        """	
        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
