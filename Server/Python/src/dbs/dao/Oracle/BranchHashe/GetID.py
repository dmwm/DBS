#!/usr/bin/env python
"""
This module provides BranchHashe.GetID data access object.
Light dao object to get the id for a given BranchHash
"""
__revision__ = "$Id: GetID.py,v 1.5 2010/03/05 15:41:53 yuyi Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    FileType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT BH.BRANCH_HASH_ID, BH.HASH
FROM %sBRANCH_HASHES BH 
""" %  self.owner 
        
    def execute(self, conn, name, transaction = False):
        """
        returns id for a given branch hash
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/BranchHashes/Insert expects db connection from up layer.")
        sql = self.sql
        sql += "WHERE BH.HASH = :branch_hash"
        binds = {"branch_hash":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Branch Hash %s does not exist" % name
        return plist[0]["branch_hash_id"]
