#!/usr/bin/env python
"""
This module provides PrimaryDataset.Insert data access object.
"""
__revision__ = "$Id: Insert.py,v 1.3 2009/10/17 17:57:50 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """ PrimaryDataset Insert DAO Class."""

    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
INSERT INTO %sPRIMARY_DATASETS
(PRIMARY_DS_ID, PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) 
VALUES(:primarydsid, :primarydsname, :primarydstypeid, :creationdate, :createby)
""" % (self.owner)

    def execute(self, inputdict, conn=None, transaction=False):
        """
        inputdict must be validated to have the following keys:
        primarydsid, primarydsname, primarydstypeid, creationdate, createby
        """
        self.dbi.processData(self.sql, inputdict, conn, transaction)
