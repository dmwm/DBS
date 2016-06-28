#!/usr/bin/env python
""" DAO Object for DatasetRuns table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 

            self.sql = """INSERT INTO %sDATASET_RUNS ( DATASET_RUN_ID, DATASET_ID, RUN_NUMBER, COMPLETE, LUMI_SECTION_COUNT, CREATION_DATE, CREATE_BY) 
			    VALUES (:datasetrunid, :datasetid, :runnumber, :complete, :lumisectioncount, :creationdate, :createby)""" % (self.owner) 

    def execute( self, conn, datasetrunsObj, transaction=False ):
	result = self.dbi.processData(self.sql, datasetrunsObj, conn, transaction)
	return


