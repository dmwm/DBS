#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.12 $"
__version__  = "$Id: Insert.py,v 1.12 2010/08/25 21:41:52 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

from sqlalchemy import exceptions

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
        file_parent_id, this_file_id, parent_file_id
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/FileParent/Insert. Expects db connection from upper layer.")

        self.dbi.processData(self.sql, daoinput, conn, transaction)
