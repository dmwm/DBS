#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/12/21 21:05:42 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, VERSION) VALUES (:release_version_id, :version)""" % (self.owner)

    def execute( self, relVerObj, conn=None, transaction=False ):
            result = self.dbi.processData(self.sql, relVerObj, conn, transaction)
            return


