#!/usr/bin/env python
"""
This module provides status update.
"""
__revision__ = "$Id: UpdateRequestStatus.py,v 1.1 2010/08/18 18:57:11 yuyi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class UpdateRequestStatus(DBFormatter):
    """
    Migration UpdateRequestStatus DAO class.
    migration_status: 
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED
        status change: 
        0 -> 1
        1 -> 2
        1 -> 3
        are only allowed changes.

    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""UPDATE %sMIGRATION_REQUESTS 
SET MIGRATION_STATUS=:migration_status, 
LAST_MODIFICATION_DATE=:last_modification_date
WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    required keys:
	    migration_status, migration_request_id, threadID
        """	
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationRequests/UpdateRequestStatus. Expects db connection from upper layer.")
        if daoinput['migration_status'] == 1:
           sql = self.sql + " and MIGRATION_STATUS = 0 "
        elif daoinput['migration_status'] == 2 or daoinput['migration_status'] == 3:
            sql = self.sql + " and MIGRATION_STATUS = 1 "
        else:
            dbsExceptionHandler("dbsException-conflict-data", "Oracle/MigrationRequest/UpdateRequestStatus. Expected migration status to be 1, 2 or 3")
        
	#binds = {"migration_status":migration_status, "threadID":threadID,
	#"last_mod_date":last_mod_date,
	# "migration_request_id":migration_request_id}
	result = self.dbi.processData(self.sql, daoinput, conn, transaction)
