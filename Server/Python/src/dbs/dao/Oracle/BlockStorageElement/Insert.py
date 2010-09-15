# DAO Object for BlockStorageElement table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:24 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO BLOCK_STORAGE_ELEMENTS(BLOCK_SE_ID, SE_ID, BLOCK_ID) VALUES (:blockseid, :seid, :blockid);"""

    def getBinds( self, block_storage_elementsObj ):
            binds = {}
            if type(block_storage_elementsObj) == type ('object'):
            	binds = {
			'blockseid' : block_storage_elementsObj['blockseid'],
			'seid' : block_storage_elementsObj['seid'],
			'blockid' : block_storage_elementsObj['blockid'],
                 }

            elif type(block_storage_elementsObj) == type([]):
               binds = []
               for item in block_storage_elementsObj:
                   binds.append({
 	                'blockseid' : item['blockseid'],
 	                'seid' : item['seid'],
 	                'blockid' : item['blockid'],
 	                })
               return binds


    def execute( self, block_storage_elementsObj ):
            binds = self.getBinds(block_storage_elementsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return