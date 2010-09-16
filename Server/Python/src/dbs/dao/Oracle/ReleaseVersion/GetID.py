#!/usr/bin/env python
"""
This module provides ReleaseVersion.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.3 2010/01/21 20:04:25 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    File GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
	"""
	SELECT R.RELEASE_VERSION_ID
	FROM %sRELEASE_VERSIONS R WHERE RELEASE_VERSION = :release_version
	""" % ( self.owner )
        
    def execute(self, release_version, conn = None, transaction = False):
        """
        returns id for a given application
        """	
        binds = {"release_version":release_version}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "release_version %s does not exist" % release_version
        return plist[0]["release_version_id"]

