# DAO Object for DatasetRun table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:26 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO DATASET_RUNS(DATASET_RUN_ID, DATASET_ID, RUN_NUMBER, COMPLETE, LUMI_SECTION_COUNT, CREATION_DATE, CREATE_BY) VALUES (:datasetrunid, :datasetid, :runnumber, :complete, :lumisectioncount, :creationdate, :createby);"""

    def getBinds( self, dataset_runsObj ):
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


    def execute( self, dataset_runsObj ):
            binds = self.getBinds(dataset_runsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return