#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2009/10/15 15:35:34 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDSType List DAO class.
    """
    sql = \
"""
SELECT PT.PRIMARY_DS_TYPE_ID, PT.PRIMARY_DS_TYPE
FROM PRIMARY_DS_TYPES PT 
""" 

    def execute(self, pattern = "", conn = None, transaction = False):
        """
        Lists all primary dataset types if pattern is not provided.
        """
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += "WHERE PT.PRIMARY_DS_TYPE = :primdstype" 
            binds = {"primdstype":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
