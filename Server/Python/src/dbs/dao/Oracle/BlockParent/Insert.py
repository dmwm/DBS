#!/usr/bin/env python
""" DAO Object for BlockParents table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql =\
                  """insert into %sblock_parents (this_block_id, parent_block_id) 
                          values (:this_block_id, 
                                  (select block_id as parent_block_id from %sblocks where block_name=:block_name) )
                  """%((self.owner,)*2)

    def execute( self, conn, binds, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/BlockParent/Insert. Expects db connection from upper layer.", self.logger.exception)
            
	result = self.dbi.processData(self.sql, binds, conn, transaction)

