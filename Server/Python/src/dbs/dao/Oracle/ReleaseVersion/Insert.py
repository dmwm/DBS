#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

__revision__ = "$Revision: 1.11 $"
__version__  = "$Id: Insert.py,v 1.11 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, RELEASE_VERSION) VALUES (:release_version_id, :release_version)""" % (self.owner)

    def execute( self, conn, relVerObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ReleaseVersion/Insert expects db connection from upper layer.")
	try:
            result = self.dbi.processData(self.sql, relVerObj, conn, transaction)
 	except Exception, ex:
	    if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		pass
	    else:
		raise

