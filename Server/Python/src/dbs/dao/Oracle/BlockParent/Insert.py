#!/usr/bin/env python
""" DAO Object for BlockParents table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:17 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sBLOCK_PARENTS ( BLOCK_PARENT_ID, THIS_BLOCK_ID, PARENT_BLOCK_ID) VALUES (:blockparentid, :thisblockid, :parentblockid) % (self.owner) ;"""

    def getBinds_delme( self, block_parentsObj ):
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


    def execute( self, block_parentsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( block_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


