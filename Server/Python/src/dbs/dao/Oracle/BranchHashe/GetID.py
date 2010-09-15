#!/usr/bin/env python
"""
This module provides BranchHashe.GetID data access object.
Light dao object to get the id for a given BranchHash
"""
__revision__ = "$Id: GetID.py,v 1.2 2009/11/24 10:58:14 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

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
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT BH.BRANCH_HASH_ID, BH.HASH
FROM %sBRANCH_HASHES BH 
""" %  self.owner 
        
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given branch hash
        """	
        sql = self.sql
        sql += "WHERE BH.HASH = :bhash"
        binds = {"bhash":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Branch Hash %s does not exist" % name
        return plist[0]["branch_hash_id"]
