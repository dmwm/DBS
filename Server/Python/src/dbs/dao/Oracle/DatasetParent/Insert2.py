#!/usr/bin/env python
""" DAO Object for DatasetParents table
    The input dictionary must have following keys:
    this_dataset_id(int)
    parent_logical_file_name(str)
""" 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql = \
                    """insert into %sdataset_parents (this_dataset_id, parent_dataset_id) 
                       values(:this_dataset_id,
                             (select dataset_id as parent_dataset_id from %sfiles where logical_file_name=:parent_logical_file_name))
                    """ % ((self.owner,)*2)

    def execute( self, conn, binds, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/DatasetParent/Insert2. Expects db connection from upper layer.")
            
	result = self.dbi.processData(self.sql, binds, conn, transaction)

