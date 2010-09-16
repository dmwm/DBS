#!/usr/bin/env python
""" DAO Object for FileBuffer table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/07/09 14:41:00 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """File Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sFILE_BUFFERS (LOGICAL_FILE_NAME, BLOCK_ID, FILE_BLOB) values (:logical_file_name, :block_id, :file_blob)""" % self.owner

    def execute(self, conn, logical_file_name, block_id, file_blob, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/FileBuffer/Insert expects db connection from upper layer.")
	binds={}
	binds['logical_file_name']=logical_file_name
	binds['file_blob']=str(file_blob)
	binds['block_id']=block_id
	self.dbi.processData(self.sql, binds, conn, transaction)
    
