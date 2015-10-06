#!/usr/bin/env python
""" DAO Object for BlockParents table
    The input dictionary must have following keys:
    this_block_id(int)
    parent_logical_file_name(str). 
    Block parentage is gotten from file parentage.
""" 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql = \
                    """insert into %sblock_parents (this_block_id, parent_block_id) 
                       values(:this_block_id,
                             (select block_id as parent_block_id from %sfiles where logical_file_name=:parent_logical_file_name))
                    """ % ((self.owner,)*2)

    def execute( self, conn, binds, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/BlockParent/Insert2. Expects db connection from upper layer.")
        bind = {}
        bindlist=[]
        if isinstance(binds, dict):
            self.dbi.processData(self.sql, binds, conn, transaction)
        elif isinstance(binds, list):
            for pf in binds:
                bind = {"this_block_id":pf["this_block_id"], "parent_logical_file_name": pf["parent_logical_file_name"]}
                bindlist.append(bind)
            self.dbi.processData(self.sql, bindlist, conn, transaction)
        else:
            dbsExceptionHandler('dbsException-invalid-input2', "Block id and parent lfn are required for BlockParent insert dao.")
