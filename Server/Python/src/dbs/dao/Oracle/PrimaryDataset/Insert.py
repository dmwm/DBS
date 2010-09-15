#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2009/11/24 10:58:15 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """ PrimaryDataset Insert DAO Class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
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
        self.dbi.processData(self.sql, daoinput, conn, transaction)
