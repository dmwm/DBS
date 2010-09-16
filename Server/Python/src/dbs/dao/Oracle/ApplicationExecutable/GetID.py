#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/12/21 21:05:38 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
	SELECT A.APP_EXEC_ID
	FROM %sAPPLICATION_EXECUTABLES A WHERE A.APP_NAME = :app_name
	""" % ( self.owner )
        
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given application
        """	

        binds = {"app_name":name}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "app %s does not exist" % name
        return plist[0]["app_exec_id"]

