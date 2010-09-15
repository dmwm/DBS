# DAO Object for DatasetType table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:26 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO DATASET_TYPES(DATASET_TYPE_ID, DATASET_TYPE) VALUES (:datasettypeid, :datasettype);"""

    def getBinds( self, dataset_typesObj ):
            binds = {}
            if type(dataset_typesObj) == type ('object'):
            	binds = {
			'datasettypeid' : dataset_typesObj['datasettypeid'],
			'datasettype' : dataset_typesObj['datasettype'],
                 }

            elif type(dataset_typesObj) == type([]):
               binds = []
               for item in dataset_typesObj:
                   binds.append({
 	                'datasettypeid' : item['datasettypeid'],
 	                'datasettype' : item['datasettype'],
 	                })
               return binds


    def execute( self, dataset_typesObj ):
            binds = self.getBinds(dataset_typesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return