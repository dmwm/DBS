#!/usr/bin/env python
""" DAO Object for FileParents table """ 
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
                    """insert into %sfile_parents 
                       (this_file_id, parent_file_id) 
                       values(:this_file_id, (select file_id from %sfiles where logical_file_name=:parent_logical_file_name))
                    """ % ((self.owner,)*2)

    def execute( self, conn, daoinput, transaction = False ):
        """
        daoinput must be validated to have the following keys:
        this_file_id, parent_logical_file_name
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileParent/Insert. Expects db connection from upper layer.")
        binds = {} 
        bindlist=[]
        if isinstance(daoinput, dict):
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        elif isinstance(daoinput, list):
            for pf in daoinput:
                binds = {"this_file_id":pf["this_file_id"], "parent_logical_file_name": pf["parent_logical_file_name"]}
                bindlist.append(binds) 
            self.dbi.processData(self.sql, bindlist, conn, transaction)
        else:
            dbsExceptionHandler('dbsException-invalid-input2', "file id and parent lfn are required for FileParent insert dao.") 
