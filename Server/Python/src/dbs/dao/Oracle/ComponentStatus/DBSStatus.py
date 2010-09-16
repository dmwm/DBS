#!/usr/bin/env python
""" DAO Object for DBS_VERSIONS Table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: DBSStatus.py,v 1.1 2010/06/14 15:12:06 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class DBSStatus(DBFormatter):
    """DBS_VERSIONS List DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """SELECT SCHEMA_VERSION, DBS_RELEASE_VERSION, INSTANCE_NAME, INSTANCE_TYPE, CREATION_DATE, LAST_MODIFICATION_DATE FROM %sDBS_VERSIONS""" % self.owner
	
    def execute(self, conn, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ComponentStatus/DBSStatus expects db connection from up layer.")
	binds={}
	result = self.dbi.processData(self.sql, binds, conn, transaction)
        return self.formatDict(result)
