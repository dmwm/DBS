#!/usr/bin/env python
""" DAO Object for DatasetTypes table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sDATASET_TYPES ( DATASET_TYPE_ID, DATASET_TYPE) VALUES (:datasettypeid, :datasettype) % (self.owner) ;"""

    def getBinds_delme( self, dataset_typesObj ):
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


    def execute( self, dataset_typesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( dataset_typesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


