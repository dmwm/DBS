#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/11/16 21:44:47 afaq Exp $ "

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
PHYSICS_GROUP_ID, 
XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, 
LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES(:datasetid, :dataset, :isdatasetvalid, 
:primaryds, :processedds, :datatier, :datasettype, 
:physicsgroup, 
:xtcrosssection, :globaltag, :creationdate, :createby, 
:lastmodificationdate, :lastmodifiedby)
""" % self.owner

# Removed    
#ACQUISITION_ERA_ID, PROCESSING_ERA_ID, 
#:processingversion
#:acquisitionera, 


#acquisitionera(id), processingversion(id),

    def execute(self, daoinput, conn = None, transaction = False):
        """
        daoinput must be a dictionary with the following keys:
        datasetid, dataset, isdatasetvalid, primaryds(id), processedds(id),
        datatier(id), datasettype(id), 
        physicsgroup(id), xtcrosssection, globaltag, 
        creationdate, createby, 
        lastmodificationdate, lastmodifiedby"""
        
        self.dbi.processData(self.sql, daoinput, conn, transaction)
