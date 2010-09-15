# DAO Object for BlockParent table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:24 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO BLOCK_PARENTS(BLOCK_PARENT_ID, THIS_BLOCK_ID, PARENT_BLOCK_ID) VALUES (:blockparentid, :thisblockid, :parentblockid);"""

    def getBinds( self, block_parentsObj ):
            binds = {}
            if type(block_parentsObj) == type ('object'):
            	binds = {
			'blockparentid' : block_parentsObj['blockparentid'],
			'thisblockid' : block_parentsObj['thisblockid'],
			'parentblockid' : block_parentsObj['parentblockid'],
                 }

            elif type(block_parentsObj) == type([]):
               binds = []
               for item in block_parentsObj:
                   binds.append({
 	                'blockparentid' : item['blockparentid'],
 	                'thisblockid' : item['thisblockid'],
 	                'parentblockid' : item['parentblockid'],
 	                })
               return binds


    def execute( self, block_parentsObj ):
            binds = self.getBinds(block_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return