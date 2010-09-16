#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2009/11/24 14:31:10 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """ PrimaryDataset Insert DAO Class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger
        self.sql = \
"""
INSERT INTO %sPRIMARY_DATASETS 
(PRIMARY_DS_ID, PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) 
VALUES(:primarydsid, :primarydsname, :primarydstypeid, :creationdate, :createby)
""" % self.owner

    def execute(self, daoinput, conn = None, transaction = False):
        """
        inputdict must be validated to have the following keys:
        primarydsid, primarydsname, primarydstypeid, creationdate, createby
        """
        
        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
            self.logger.warning("Unique constraint violation being ignored...")
            self.logger.warning("%s" % ex)
