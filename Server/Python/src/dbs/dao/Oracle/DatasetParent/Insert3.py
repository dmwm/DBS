#!/usr/bin/env python
""" DAO Object for DatasetParents table
    The input dictionary must have following keys:
    this_dataset_id(int)
    parent_dataset(str).
""" 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert3(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql = \
                    """insert into %sdataset_parents (this_dataset_id, parent_dataset_id) 
                       values(:this_dataset_id,
                             (select dataset_id as parent_dataset_id from %sdatasets where dataset=:parent_dataset))
                    """ % ((self.owner,)*2)

    def execute( self, conn, binds, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/DatasetParent/Insert2. Expects db connection from upper layer.", self.logger.exception)
        
        bind = {}
        bindlist=[]
        if isinstance(binds, dict):
            self.dbi.processData(self.sql, binds, conn, transaction)
        elif isinstance(binds, list):
            for pf in binds:
                bind = {"this_dataset_id":pf["this_dataset_id"], "parent_dataset": pf["parent_dataset"]}
                bindlist.append(bind)
            self.dbi.processData(self.sql, bindlist, conn, transaction)
        else:
            dbsExceptionHandler('dbsException-invalid-input2', "Dataset id and parent dataset are required for DatasetParent insert dao.", self.logger.exception)


