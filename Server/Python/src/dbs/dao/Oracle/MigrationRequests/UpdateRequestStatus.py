#!/usr/bin/env python
"""
This module provides status update.
"""
__revision__ = "$Id: UpdateRequestStatus.py,v 1.1 2010/08/18 18:57:11 yuyi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
class UpdateRequestStatus(DBFormatter):
    """
    Migration UpdateRequestStatus DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""UPDATE %sMIGRATION_REQUESTS 
SET MIGRATION_STATUS=:migration_status, LAST_MODIFIED_BY=:threadID ,
LAST_MODIFICATION_DATE=:last_mod_date
WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %  self.owner 
        
    def execute(self, conn, migration_request_id, migration_status, threadID, last_mod_date, transaction = False):
        """
	    required keys:
	    migration_status, migration_request_id, threadID
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Migration_request/UpdateRequestStatus expects db connection from upper layer.")
	binds = {"migration_status":migration_status, "threadID":threadID,
	"last_mod_date":last_mod_date,
	 "migration_request_id":migration_request_id}
	result = self.dbi.processData(self.sql, binds, conn, transaction)
