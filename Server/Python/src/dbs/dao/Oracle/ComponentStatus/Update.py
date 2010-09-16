#!/usr/bin/env python
""" DAO Object for ComponentStatus Table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Update.py,v 1.3 2010/06/23 21:21:20 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Update(DBFormatter):
    """ComponentStatus Update DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """UPDATE %sCOMPONENT_STATUS SET COMPONENT_STATUS=:component_status, LAST_CONTACT_TIME=:last_contact_time WHERE COMPONENT_NAME=:component_name""" % self.owner
	
    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ComponentStatus/Update expects db connection from upper layer.")
	self.dbi.processData(self.sql, daoinput, conn, transaction)
