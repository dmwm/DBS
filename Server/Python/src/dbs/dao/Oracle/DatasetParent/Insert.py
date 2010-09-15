#!/usr/bin/env python
""" DAO Object for DatasetParents table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sDATASET_PARENTS ( DATASET_PARENT_ID, THIS_DATASET_ID, PARENT_DATASET_ID) VALUES (:datasetparentid, :thisdatasetid, :parentdatasetid) % (self.owner) ;"""

    def getBinds_delme( self, dataset_parentsObj ):
            binds = {}
            if type(dataset_parentsObj) == type ('object'):
            	binds = {
			'datasetparentid' : dataset_parentsObj['datasetparentid'],
			'thisdatasetid' : dataset_parentsObj['thisdatasetid'],
			'parentdatasetid' : dataset_parentsObj['parentdatasetid'],
                 }

            elif type(dataset_parentsObj) == type([]):
               binds = []
               for item in dataset_parentsObj:
                   binds.append({
 	                'datasetparentid' : item['datasetparentid'],
 	                'thisdatasetid' : item['thisdatasetid'],
 	                'parentdatasetid' : item['parentdatasetid'],
 	                })
               return binds


    def execute( self, dataset_parentsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( dataset_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


