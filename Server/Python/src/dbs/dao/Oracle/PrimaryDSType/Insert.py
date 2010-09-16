#!/usr/bin/env python
""" DAO Object for PrimaryDSTypes table """ 

__revision__ = "$Revision: 1.5 $"
__version__  = "$Id: Insert.py,v 1.5 2010/03/05 19:31:45 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    
            self.sql = """INSERT INTO %sPRIMARY_DS_TYPES ( PRIMARY_DS_TYPE_ID, PRIMARY_DS_TYPE) VALUES (:primarydstypeid, :primarydstype)""" % (self.owner)

    def execute( self, conn, primary_ds_typesObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/PrimaryDSType/Insert expects db connection from up layer.")
	result = self.dbi.processData(self.sql, primary_ds_typesObj, conn, transaction)
	return


