#!/usr/bin/env python
""" DAO Object for DatasetParents table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:22 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    self.logger = logger

            self.sql = """INSERT INTO %sDATASET_PARENTS ( DATASET_PARENT_ID, THIS_DATASET_ID, PARENT_DATASET_ID) VALUES (:dataset_parent_id, :this_dataset_id, :parent_dataset_id)""" % (self.owner)
	    
    def execute( self, conn, binds, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/DatasetParent/Inser expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, binds, conn, transaction)


