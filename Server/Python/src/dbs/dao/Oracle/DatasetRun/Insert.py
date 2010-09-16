#!/usr/bin/env python
""" DAO Object for DatasetRuns table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/28 23:08:01 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sDATASET_RUNS ( DATASET_RUN_ID, DATASET_ID, RUN_NUMBER, COMPLETE, LUMI_SECTION_COUNT, CREATION_DATE, CREATE_BY) VALUES (:datasetrunid, :datasetid, :runnumber, :complete, :lumisectioncount, :creationdate, :createby)""" % (self.owner) 

    def getBinds_delme( self, dataset_runsObj ):
            binds = {}
            if type(dataset_runsObj) == type ('object'):
            	binds = {
			'datasetrunid' : dataset_runsObj['datasetrunid'],
			'datasetid' : dataset_runsObj['datasetid'],
			'runnumber' : dataset_runsObj['runnumber'],
			'complete' : dataset_runsObj['complete'],
			'lumisectioncount' : dataset_runsObj['lumisectioncount'],
			'creationdate' : dataset_runsObj['creationdate'],
			'createby' : dataset_runsObj['createby'],
                 }

            elif type(dataset_runsObj) == type([]):
               binds = []
               for item in dataset_runsObj:
                   binds.append({
 	                'datasetrunid' : item['datasetrunid'],
 	                'datasetid' : item['datasetid'],
 	                'runnumber' : item['runnumber'],
 	                'complete' : item['complete'],
 	                'lumisectioncount' : item['lumisectioncount'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                })
               return binds


    def execute( self, dataset_runsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( dataset_runsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


