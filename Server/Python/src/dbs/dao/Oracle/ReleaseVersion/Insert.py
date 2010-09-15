#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2010/01/27 15:07:50 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, RELEASE_VERSION) VALUES (:release_version_id, :release_version)""" % (self.owner)

    def execute( self, relVerObj, conn=None, transaction=False ):
	try:
            result = self.dbi.processData(self.sql, relVerObj, conn, transaction)
 	except Exception, ex:
	    if str(ex).lower().find("unique constraint") != -1 :
		pass
	    else:
		raise

