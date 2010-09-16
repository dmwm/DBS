#!/usr/bin/env python
""" DAO Object for DatasetTypes table """ 

__revision__ = "$Revision: 1.10 $"
__version__  = "$Id: Insert.py,v 1.10 2010/09/14 14:34:11 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sDATASET_ACCESS_TYPES ( DATASET_ACCESS_TYPE_ID, DATASET_ACCESS_TYPE) VALUES
            (:dataset_access_type_id, :dataset_access_type)""" % (self.owner)


    def execute( self, conn, dataset_typesObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/DatasetType/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, dataset_typesObj, conn, transaction)
	return


