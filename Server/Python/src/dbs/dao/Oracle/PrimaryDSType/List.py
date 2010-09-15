#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.6 2010/01/12 15:09:27 yuyi Exp $"
__version__ = "$Revision: 1.6 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDSType List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        #self.owner = "%s." % self.dbi.engine.url.username
	self.owner = "%s." %owner
        self.sql = \
"""
SELECT PT.PRIMARY_DS_TYPE_ID, PT.PRIMARY_DS_TYPE
FROM %sPRIMARY_DS_TYPES PT 
""" % (self.owner)

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
