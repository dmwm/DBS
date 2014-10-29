#!/usr/bin/env python
"""
This module provides Migration.Remove data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class Remove(DBFormatter):
    """
    Migration Update DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

	#check before delete since rowcount is not supported in wmcore 
	self.select = """
	select count(*) as count from {owner}MIGRATION_REQUESTS 
	WHERE MIGRATION_REQUEST_ID=:migration_rqst_id and create_by=:create_by 
	      and (migration_status=0 or migration_status=3 or migration_status=9)
        """.format(owner=self.owner)

        self.sql = """
	Delete from %sMIGRATION_REQUESTS  
	WHERE MIGRATION_REQUEST_ID=:migration_rqst_id  
	""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_request_id
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationRequests/Remove. Expects db connection from upper layer.")
        daoinput['create_by'] = dbsUtils().getCreateBy()
	try:
            msg = "DBSMigration: Invalid request. Sucessfully processed or processing requests cannot be removed,\
                    or the requested migration did not exist, or the requestor for removing and creating has to be the same user. "
            checkit = self.dbi.processData(self.select, daoinput, conn, transaction)
            if self.formatDict(checkit)[0]["count"] >= 1:
	        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
            else:
                dbsExceptionHandler('dbsException-invalid-input', msg)
	except:
            raise
