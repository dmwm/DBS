#!/usr/bin/env python
"""
This module provides PrimaryDataset.Insert data access object.
"""
__revision__ = "$Id: Insert.py,v 1.2 2009/10/15 15:32:37 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """
    PrimaryDataset Insert DAO Class.
    """

    sql = \
"""
INSERT INTO PRIMARY_DATASETS
(PRIMARY_DS_ID, PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) 
VALUES(:primarydsid, :primarydsname, :primarydstypeid, :creationdate, :createby)
"""

    def execute(self, primdsobject, conn=None, transaction=False):
        """
        primdsobject dictionary has to have the following keys:
        primarydsid, primarydsname, primarydstypeid, creationdate, createby
        """
        binds = primdsobject
        self.dbi.processData(self.sql, binds, conn, transaction)
            
