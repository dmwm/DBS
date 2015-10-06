#!/usr/bin/env python
""" DAO Object for PrimaryDSTypes table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2010/09/14 14:38:33 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    
            self.sql = """INSERT INTO %sPRIMARY_DS_TYPES ( PRIMARY_DS_TYPE_ID, PRIMARY_DS_TYPE) VALUES (:primary_ds_type_id,
            :primary_ds_type)""" % (self.owner)

    def execute( self, conn, primary_ds_typesObj, transaction=False ):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/PrimaryDSType/Insert. Expects db connection from upper layer.")

	result = self.dbi.processData(self.sql, primary_ds_typesObj, conn, transaction)
	return


