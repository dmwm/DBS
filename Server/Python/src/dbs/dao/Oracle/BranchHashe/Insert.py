!/usr/bin/env python
""" DAO Object for BranchHashes table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/11/24 10:58:14 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sBRANCH_HASHES ( BRANCH_HASH_ID, HASH, CONTENT) VALUES (:branchhashid, :hash, :content) % (self.owner) ;"""

    def getBinds_delme( self, branch_hashesObj ):
            binds = {}
            if type(branch_hashesObj) == type ('object'):
            	binds = {
			'branchhashid' : branch_hashesObj['branchhashid'],
			'hash' : branch_hashesObj['hash'],
			'content' : branch_hashesObj['content'],
                 }

            elif type(branch_hashesObj) == type([]):
               binds = []
               for item in branch_hashesObj:
                   binds.append({
 	                'branchhashid' : item['branchhashid'],
 	                'hash' : item['hash'],
 	                'content' : item['content'],
 	                })
               return binds


    def execute( self, branch_hashesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( branch_hashesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


