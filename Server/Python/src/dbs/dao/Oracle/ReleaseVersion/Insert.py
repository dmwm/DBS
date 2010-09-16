#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2010/01/07 17:30:43 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, VERSION) VALUES (:release_version_id, :release_version)""" % (self.owner)

    def execute( self, relVerObj, conn=None, transaction=False ):
            result = self.dbi.processData(self.sql, relVerObj, conn, transaction)
            return


