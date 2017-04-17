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


        self.sql1 = \
"""
insert into %sfile_lumis 
(run_num, lumi_section_num, file_id) 
values (:run_num, :lumi_section_num, :file_id)
""" % (self.owner)

    def execute( self, conn, daoinput, transaction = False ):
        eventC = False
        if isinstance(daoinput,list):
            if "event_count" in daoinput[0]:
                eventC = True
        else:
            if "event_count" in daoinput:
                eventC = Ture
        if eventC:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        else:
            self.dbi.processData(self.sql1, daoinput, conn, transaction)
