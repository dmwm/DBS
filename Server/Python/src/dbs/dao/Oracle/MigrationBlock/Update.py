#!/usr/bin/env python:wq
"""
This module provides Migration.Update data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Update(DBFormatter):
    """
    Migration Update DAO class.
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
        are only allowed changes for working through.
        3 -> 1 allowed for retrying when retry_count <3.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
                        """UPDATE %sMIGRATION_BLOCKS
                           SET 
                                MIGRATION_STATUS=:migration_status , 
                                LAST_MODIFICATION_DATE=:last_modification_date 
                           WHERE """ %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_block_id, migration_request_id
        """
        #print daoinput['migration_block_id']
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/MigrationBlock/Update. Expects db connection from upper layer." ,self.logger.exception)
        if daoinput['migration_status'] == 1:
           sql = self.sql + "  (MIGRATION_STATUS = 0  or MIGRATION_STATUS = 3)" 
        elif daoinput['migration_status'] == 2 or daoinput['migration_status'] == 3 or daoinput['migration_status'] == 9:
            sql = self.sql + " MIGRATION_STATUS = 1 "
        else: 
            dbsExceptionHandler("dbsException-conflict-data", "Oracle/MigrationBlock/Update. Expected migration status to be 1, 2, 3, 0r 9" ,self.logger.exception ) 
        #print sql
        if 'migration_request_id' in daoinput:
            sql3 = sql + "and MIGRATION_REQUEST_ID =:migration_request_id"
            result = self.dbi.processData(sql3, daoinput, conn, transaction)
        elif 'migration_block_id' in daoinput:
            sql2 = sql+ " and MIGRATION_BLOCK_ID =:migration_block_id"
            if type(daoinput['migration_block_id']) is not list:
                result = self.dbi.processData(sql2, daoinput, conn, transaction)
            else:
                #WMCore require the input has to be a list of dictionary in order to insert as bulk.
                newdaoinput=[]
                for id in daoinput['migration_block_id']:
                    newdaoinput.append({"migration_status":daoinput["migration_status"], "migration_block_id":id, 
                    "last_modification_date":daoinput["last_modification_date"]})
                result = self.dbi.processData(sql2, newdaoinput, conn, transaction)
        else:
            dbsExceptionHandler("dbsException-conflict-data", "Oracle/MigrationBlock/Update. Required IDs not in the input", self.logger.exception)
