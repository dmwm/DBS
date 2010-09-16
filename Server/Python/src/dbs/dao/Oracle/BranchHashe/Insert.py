!/usr/bin/env python
""" DAO Object for BranchHashes table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2010/06/23 21:21:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
           

            self.sql = """INSERT INTO %sBRANCH_HASHES ( BRANCH_HASH_ID, HASH, CONTENT) VALUES (:branch_hash_id, :branch_hash, :content)""" % (self.owner)

    def execute( self, conn, binds, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/BranchHashes expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


