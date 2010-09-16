#!/usr/bin/env python
""" DAO Object for BlockStorageElements table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
            self.sql = """INSERT INTO %sBLOCK_STORAGE_ELEMENTS ( BLOCK_SE_ID, SE_ID, BLOCK_ID) VALUES (:blockseid, :seid, :blockid)""" % (self.owner)

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


    def execute( self, conn, block_storage_elementsObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/BlockStorageElement/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


