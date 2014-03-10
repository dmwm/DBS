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
        3=FAILED (will be retried)
        9=Terminally FAILED
        status change: 
        0 -> 1
        1 -> 2
        1 -> 3
        1 -> 9
        are only allowed changes for working through .
        3 -> 1 allowed for retrying with retry_count <3

    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
                    """UPDATE %sMIGRATION_REQUESTS 
                       SET 
                                MIGRATION_STATUS=:migration_status, 
                                LAST_MODIFICATION_DATE=:last_modification_date
                       WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %  self.owner 
       
        self.sql2 = """update %smigration_requests 
                       set 
                                migration_status=:migration_status, 
                                LAST_MODIFICATION_DATE=:last_modification_date, 
                                retry_count= (select (CASE migration_status 
                                                                when 3 then retry_count+1 
                                                                else 0  
                                                                END)count 
                                              from %smigration_requests where migration_request_id=:migration_request_id) 
                       where migration_request_id=:migration_request_id and (migration_status=0 or migration_status=3)""" %((self.owner,)*2)
        
        self.sql3 = """ UPDATE %smigration_requests
                        SET
                            LAST_MODIFICATION_DATE=:last_modification_date,
                            MIGRATION_STATUS = ( select (CASE retry_count
                                                            when 3 then 9
							    when 4 then 9  	
                                                            else :migration_status 
                                                        END)migration_status 
                                                 from %smigration_requests  
						 where migration_request_id=:migration_request_id
                                               ) 
                       WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %((self.owner,)*2)     

    def execute(self, conn, daoinput, transaction = False):
        """
	    required keys:
	    migration_status, migration_request_id
        """	
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationRequests/UpdateRequestStatus. Expects db connection from upper layer.")
        if daoinput['migration_status'] == 1:
           sql = self.sql2 
        elif daoinput['migration_status'] == 2:
            sql = self.sql + " and MIGRATION_STATUS = 1 "
        elif daoinput['migration_status'] == 3:
            sql = self.sql3 + " and MIGRATION_STATUS = 1 " 
        else:
            dbsExceptionHandler("dbsException-conflict-data", "Oracle/MigrationRequest/UpdateRequestStatus. Expected migration status to be 1, 2 or 3")
        
	result = self.dbi.processData(sql, daoinput, conn, transaction)
