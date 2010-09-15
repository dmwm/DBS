#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/10/30 16:47:22 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

        self.sql = \
"""
INSERT INTO %sDATASETS 
(DATASET_ID, DATASET, IS_DATASET_VALID, 
PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_TYPE_ID, 
ACQUISITION_ERA_ID, PROCESSING_ERA_ID, PHYSICS_GROUP_ID, 
XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, 
LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES(:datasetid, :dataset, :isdatasetvalid, 
:primaryds, :processedds, :datatier, :datasettype, 
:acquisitionera, :processingversion, :physicsgroup, 
:xtcrosssection, :globaltag, :creationdate, :createby, 
:lastmodificationdate, :lastmodifiedby)
""" % self.owner
    

    def execute(self, daoinput, conn = None, transaction = False):
        """
        daoinput must be a dictionary with the following keys:
        datasetid, dataset, isdatasetvalid, primaryds(id), processedds(id),
        datatier(id), datasettype(id), acquisitionera(id), processingversion(id),
        physicsgroup(id), xtcrosssection, globaltag, 
        creationdate, createby, 
        lastmodificationdate, lastmodifiedby"""
        
        self.dbi.processData(self.sql, daoinput, conn, transaction)
