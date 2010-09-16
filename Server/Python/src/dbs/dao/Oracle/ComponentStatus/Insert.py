#!/usr/bin/env python
""" DAO Object for ComponentStatus Table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/06/23 21:21:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """ComponentStatus Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sCOMPONENT_STATUS 
		(COMP_STATUS_ID, COMPONENT_NAME, COMPONENT_STATUS, LAST_CONTACT_TIME) 
	    values (:comp_status_id, :component_name, :component_status, :last_contact_time)
	""" % self.owner
	
    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ComponentStatus/Insert expects db connection from upper layer.")
	self.dbi.processData(self.sql, daoinput, conn, transaction)
