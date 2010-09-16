#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.11 $"
__version__  = "$Id: Insert.py,v 1.11 2010/02/11 22:51:59 afaq Exp $ "

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
VALUES (:FILE_LUMI_ID, :RUN_NUM, :LUMI_SECTION_NUM, :FILE_ID)
""" % (self.owner)

    def execute( self, daoinput, conn = None, transaction = False ):
        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
	    if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		self.logger.warning("Unique constraint violation being ignored...")
		self.logger.warning("%s" % ex)
	    else: 
		raise

