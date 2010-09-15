# DAO Object for ProcessedDataset table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:31 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PROCESSED_DATASETS(PROCESSED_DS_ID, PROCESSED_DS_NAME) VALUES (:processeddsid, :processeddsname);"""

    def getBinds( self, processed_datasetsObj ):
            binds = {}
            if type(processed_datasetsObj) == type ('object'):
            	binds = {
			'processeddsid' : processed_datasetsObj['processeddsid'],
			'processeddsname' : processed_datasetsObj['processeddsname'],
                 }

            elif type(processed_datasetsObj) == type([]):
               binds = []
               for item in processed_datasetsObj:
                   binds.append({
 	                'processeddsid' : item['processeddsid'],
 	                'processeddsname' : item['processeddsname'],
 	                })
               return binds


    def execute( self, processed_datasetsObj ):
            binds = self.getBinds(processed_datasetsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return