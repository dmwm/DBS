#!/usr/bin/env python
""" DAO Object for StorageElements table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/28 23:08:03 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sSTORAGE_ELEMENTS ( SE_ID, SE_NAME) VALUES (:seid, :sename)""" % (self.owner)

    def getBinds_delme( self, storage_elementsObj ):
            binds = {}
            if type(storage_elementsObj) == type ('object'):
            	binds = {
			'seid' : storage_elementsObj['seid'],
			'sename' : storage_elementsObj['sename'],
                 }

            elif type(storage_elementsObj) == type([]):
               binds = []
               for item in storage_elementsObj:
                   binds.append({
 	                'seid' : item['seid'],
 	                'sename' : item['sename'],
 	                })
               return binds


    def execute( self, storage_elementsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( storage_elementsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


