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
"""UPDATE %sMIGRATION_BLOCKS
SET MIGRATION_STATUS=:migration_status 
WHERE MIGRATION_BLOCK_ID =:migration_block_id""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_block_id
        """
        print daoinput['migration_block_id']
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationBlock/Update. Expects db connection from upper layer.")
        if daoinput['migration_status'] == 1:
           sql = self.sql + " and MIGRATION_STATUS = 0 " 
        elif daoinput['migration_status'] == 2 or daoinput['migration_status'] == 3:
            sql = self.sql + " and MIGRATION_STATUS = 1 "
        else: 
            dbsExceptionHandler("dbsException-conflict-data", "Oracle/MigrationBlock/Update. Expected migration status to be 1, 2 or 3") 
        #print sql
        if type(daoinput['migration_block_id']) is not list:
            result = self.dbi.processData(sql, daoinput, conn, transaction)
        else:
            #WMCore require the input has to be a list of dictionary in order to insert as bulk.
            newdaoinput=[]
            for id in daoinput['migration_block_id']:
                newdaoinput.append({"migration_status":daoinput["migration_status"], "migration_block_id":id})
            result = self.dbi.processData(sql, newdaoinput, conn, transaction)
