#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.5 $"
__version__  = "$Id: Insert.py,v 1.5 2009/11/19 17:37:42 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

        self.sql = \
"""
INSERT INTO %sFILE_PARENTS 
(FILE_PARENT_ID, THIS_FILE_ID, PARENT_FILE_ID) 
VALUES (:FILE_PARENT_ID, :THIS_FILE_ID, :FILE_PARENT_LFN)
""" % (self.owner)

    def execute( self, daoinput, conn = None, transaction = False ):
        """
        daoinput must be validated to have the following keys:
        FILE_PARENT_ID, THIS_FILE_ID, FILE_PARENT_LFN(ID)
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)
