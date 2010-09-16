#!/usr/bin/env python
""" DAO Object for FileBuffer table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/05/25 21:01:55 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """File Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sFILE_BUFFER (LFN, BLOCK_ID, FILE_BLOB) values (:lfn, :block_id, :file_blob)""" % self.owner

    def execute(self, conn, lfn, block_id, file_blob, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/FileBuffer/Insert expects db connection from up layer.")
	daoinput={}
	daoinput['lfn']=lfn
	daoinput['file_blob']=str(file_blob)
	daoinput['block_id']=block_id
	self.dbi.processData(self.sql, daoinput, conn, transaction)
    
