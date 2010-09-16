#!/usr/bin/env python
"""
This module provides ParameterSetHashes.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.6 2010/06/23 21:21:25 afaq Exp $"
__version__ = "$Revision: 1.6 $"

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
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT P.PARAMETER_SET_HASH_ID
	FROM %sPARAMETER_SET_HASHES P WHERE PSET_HASH = :pset_hash
	""" % ( self.owner )
        
    def execute(self, conn, pset_hash, transaction = False):
        """
        returns id for a given application
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/ParameterSetHashe/GetID expects db connection from upper layer.")
        binds = {"pset_hash":pset_hash}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "PARAMETER_SET_HASH %s does not exist" % pset_hash
        return plist[0]["parameter_set_hash_id"]

