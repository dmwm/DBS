#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.15 $"
__version__  = "$Id: Insert.py,v 1.15 2010/08/25 21:41:52 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
INSERT INTO %sFILE_LUMIS 
(FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) 
VALUES (:file_lumi_id, :run_num, :lumi_section_num, :file_id)
""" % (self.owner)

    def execute( self, conn, daoinput, transaction = False ):
	self.dbi.processData(self.sql, daoinput, conn, transaction)

