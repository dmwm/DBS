#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.12 $"
__version__  = "$Id: Insert.py,v 1.12 2010/08/25 21:41:52 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
INSERT INTO %sFILE_PARENTS 
(FILE_PARENT_ID, THIS_FILE_ID, PARENT_FILE_ID) 
VALUES (:file_parent_id, :this_file_id, :parent_file_id)
""" % (self.owner)

    def execute( self, conn, daoinput, transaction = False ):
        """
        daoinput must be validated to have the following keys:
        file_parent_id, this_file_id, parent_file_id
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)
