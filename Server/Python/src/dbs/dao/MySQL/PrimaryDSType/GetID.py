#!/usr/bin/env python
"""
This module provides PrimaryDSType.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2010/02/05 21:00:46 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    PrimaryDSType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT PT.PRIMARY_DS_TYPE_ID, PT.PRIMARY_DS_TYPE
FROM %sPRIMARY_DS_TYPES PT 
""" % (self.owner)

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a give primdstype
        """
        sql = self.sql
        sql += "WHERE PT.PRIMARY_DS_TYPE = :primdstype" 
        binds = {"primdstype":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "PrimaryDSType %s does not exist" % name
        return plist[0]["primary_ds_type_id"]

