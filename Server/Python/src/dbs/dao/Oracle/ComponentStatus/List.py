#!/usr/bin/env python
""" DAO Object for ComponentStatus Table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: List.py,v 1.1 2010/06/14 14:32:54 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class List(DBFormatter):
    """ComponentStatus List DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """SELECT COMPONENT_NAME, COMPONENT_STATUS, LAST_CONTACT_TIME FROM %sCOMPONENT_STATUS""" % self.owner
	
    def execute(self, conn, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ComponentStatus/List expects db connection from up layer.")
	binds={}
	result = self.dbi.processData(self.sql, binds, conn, transaction)
        return self.formatDict(result)
