#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.15 $"
__version__  = "$Id: Insert.py,v 1.15 2010/08/25 21:41:52 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

from sqlalchemy import exceptions

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
insert into %sfile_lumis 
(file_lumi_id , run_num, lumi_section_num, file_id) 
values (%sseq_flm.nextval, :run_num, :lumi_section_num, :file_id)
""" % ((self.owner,)*2)

    def execute( self, conn, daoinput, transaction = False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/FileLumi/Insert. Expects db connection from upper layer.")

	self.dbi.processData(self.sql, daoinput, conn, transaction)

