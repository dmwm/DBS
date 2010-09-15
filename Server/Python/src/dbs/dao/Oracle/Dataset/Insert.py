#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:18 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sDATASETS ( DATASET_ID, DATASET, IS_DATASET_VALID, PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_TYPE_ID, ACQUISITION_ERA_ID, PROCESSING_ERA_ID, PHYSICS_GROUP_ID, XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES (:datasetid, :dataset, :isdatasetvalid, :primarydsid, :processeddsid, :datatierid, :datasettypeid, :acquisitioneraid, :processingeraid, :physicsgroupid, :xtcrosssection, :globaltag, :creationdate, :createby, :lastmodificationdate, :lastmodifiedby) % (self.owner) ;"""

    def getBinds_delme( self, datasetsObj ):
            binds = {}
            if type(datasetsObj) == type ('object'):
            	binds = {
			'datasetid' : datasetsObj['datasetid'],
			'dataset' : datasetsObj['dataset'],
			'isdatasetvalid' : datasetsObj['isdatasetvalid'],
			'primarydsid' : datasetsObj['primarydsid'],
			'processeddsid' : datasetsObj['processeddsid'],
			'datatierid' : datasetsObj['datatierid'],
			'datasettypeid' : datasetsObj['datasettypeid'],
			'acquisitioneraid' : datasetsObj['acquisitioneraid'],
			'processingeraid' : datasetsObj['processingeraid'],
			'physicsgroupid' : datasetsObj['physicsgroupid'],
			'xtcrosssection' : datasetsObj['xtcrosssection'],
			'globaltag' : datasetsObj['globaltag'],
			'creationdate' : datasetsObj['creationdate'],
			'createby' : datasetsObj['createby'],
			'lastmodificationdate' : datasetsObj['lastmodificationdate'],
			'lastmodifiedby' : datasetsObj['lastmodifiedby'],
                 }

            elif type(datasetsObj) == type([]):
               binds = []
               for item in datasetsObj:
                   binds.append({
 	                'datasetid' : item['datasetid'],
 	                'dataset' : item['dataset'],
 	                'isdatasetvalid' : item['isdatasetvalid'],
 	                'primarydsid' : item['primarydsid'],
 	                'processeddsid' : item['processeddsid'],
 	                'datatierid' : item['datatierid'],
 	                'datasettypeid' : item['datasettypeid'],
 	                'acquisitioneraid' : item['acquisitioneraid'],
 	                'processingeraid' : item['processingeraid'],
 	                'physicsgroupid' : item['physicsgroupid'],
 	                'xtcrosssection' : item['xtcrosssection'],
 	                'globaltag' : item['globaltag'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'lastmodificationdate' : item['lastmodificationdate'],
 	                'lastmodifiedby' : item['lastmodifiedby'],
 	                })
               return binds


    def execute( self, datasetsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( datasetsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


