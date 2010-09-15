#!/usr/bin/env python
"""
This module provides ParameterSetHashes.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/12/21 21:05:40 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    ParameterSetHashes GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
	"""
	SELECT P.PARAMETER_SET_HASH_ID
	FROM %sPARAMETER_SET_HASHES P WHERE HASH = :hash
	""" % ( self.owner )
        
    def execute(self, hash, conn = None, transaction = False):
        """
        returns id for a given application
        """	
        binds = {"hash":hash}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "PARAMETER_SET_HASH %s does not exist" % hash
        return plist[0]["parameter_set_hash_id"]

