#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2009/11/27 09:55:03 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger
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

    def execute(self, daoinput, conn = None, transaction = False):
        """
        daoinput must be a dictionary with the following keys:
        datasetid, dataset, isdatasetvalid, primaryds(id), processedds(id),
        datatier(id), datasettype(id), 
        physicsgroup(id), xtcrosssection, globaltag, 
        creationdate, createby, 
        lastmodificationdate, lastmodifiedby"""

        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
            self.logger.warning("Unique constraint violation being ignored...")
            self.logger.warning("%s" % ex)
