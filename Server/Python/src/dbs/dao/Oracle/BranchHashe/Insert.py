# DAO Object for BranchHashe table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:24 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO BRANCH_HASHES(BRANCH_HASH_ID, HASH, CONTENT) VALUES (:branchhashid, :hash, :content);"""

    def getBinds( self, branch_hashesObj ):
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


    def execute( self, branch_hashesObj ):
            binds = self.getBinds(branch_hashesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return