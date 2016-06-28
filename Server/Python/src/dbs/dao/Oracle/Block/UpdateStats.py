#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class UpdateStats(DBFormatter):
    """
    Block UpdateStats DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sBLOCKS SET FILE_COUNT=:file_count, BLOCK_SIZE=:block_size where BLOCK_ID=:block_id""" %  self.owner 
        
    def execute(self, conn, blockStats, transaction = False):
        """
        for a given block_id
        """	
	if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "dbs/dao/Oracle/Block/UpdateStatus expects db connection from upper layer.", self.logger.exception)
        result = self.dbi.processData(self.sql, blockStats, conn, transaction)
