#!/usr/bin/env python
""" DAO Object for FileParents table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
insert into %sfile_lumis 
(run_num, lumi_section_num, file_id, event_count) 
values (:run_num, :lumi_section_num, :file_id, :event_count)
""" % (self.owner)

    def execute( self, conn, daoinput, transaction = False ):
        self.dbi.processData(self.sql, daoinput, conn, transaction)

