# DAO Object for StorageElement table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:32 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO STORAGE_ELEMENTS(SE_ID, SE_NAME) VALUES (:seid, :sename);"""

    def getBinds( self, storage_elementsObj ):
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


    def execute( self, storage_elementsObj ):
            binds = self.getBinds(storage_elementsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return