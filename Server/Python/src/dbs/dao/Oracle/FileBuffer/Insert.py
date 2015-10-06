#!/usr/bin/env python
""" DAO Object for FileBuffer table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):
    """File Insert DAO Class."""
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sFILE_BUFFERS (LOGICAL_FILE_NAME, BLOCK_ID, FILE_CLOB) values (:logical_file_name, :block_id, :file_clob)""" % self.owner

    def execute(self, conn, logical_file_name, block_id, file_clob, transaction = False):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileBuffer/Insert. Expects db connection from upper layer.")
	
	binds={}
	binds['logical_file_name']=logical_file_name
	binds['file_clob']=str(file_clob)
	binds['block_id']=block_id
	self.dbi.processData(self.sql, binds, conn, transaction)
    
