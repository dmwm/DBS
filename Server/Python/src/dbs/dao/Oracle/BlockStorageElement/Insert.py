#!/usr/bin/env python
""" DAO Object for BlockStorageElements table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:18 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sBLOCK_STORAGE_ELEMENTS ( BLOCK_SE_ID, SE_ID, BLOCK_ID) VALUES (:blockseid, :seid, :blockid) % (self.owner) ;"""

    def getBinds_delme( self, block_storage_elementsObj ):
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


    def execute( self, block_storage_elementsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( block_storage_elementsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


