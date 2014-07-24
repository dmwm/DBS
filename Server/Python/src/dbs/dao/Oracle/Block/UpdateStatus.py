#!/usr/bin/env python
"""
This module provides Block.UpdateStatus data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class UpdateStatus(DBFormatter):

    """
    Block Update Status DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sBLOCKS SET OPEN_FOR_WRITING = :open_for_writing , LAST_MODIFIED_BY=:myuser,
LAST_MODIFICATION_DATE = :ltime where BLOCK_NAME = :block_name""" %  self.owner
        
    def execute ( self, conn, block_name, open_for_writing, ltime, transaction=False ):
        """
        for a given file
        """	
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed",
                                "Oracle/Block/UpdateStatus. Expects db connection from upper layer.")
        binds = {"block_name": block_name ,"open_for_writing": open_for_writing , 'ltime': ltime,
                 'myuser': dbsUtils().getCreateBy()}
        self.dbi.processData(self.sql, binds, conn, transaction)
    
