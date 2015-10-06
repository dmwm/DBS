#!/usr/bin/env python
"""
This module provides PhysicsGroup.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    PhysicsGroup List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT pg.physics_group_name
                        FROM %sPHYSICS_GROUPS pg 
                    """ %self.owner

    def execute(self, conn, name='', transaction = False):
        """
        returns id for a given physics group name
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/PhysicsGroup/List. Expects db connection from upper layer.")

        binds={}
        if name:
            op = ('=', 'like')['%' in name]
            sql = self.sql + "  WHERE pg.physics_group_name %s :physicsgroup" % (op)
            binds = {"physicsgroup": name}
        else:
            sql = self.sql
        self.logger.debug(sql)    
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        self.logger.debug(plist)
	if len(plist) < 1: return []
        return plist
