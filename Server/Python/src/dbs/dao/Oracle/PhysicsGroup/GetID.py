#!/usr/bin/env python
"""
This module provides PhysicsGroup.GetID data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class GetID(DBFormatter):
    """
    PhysicsGroup GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT pg.physics_group_id, pg.physics_group_name
                        FROM %sPHYSICS_GROUPS pg
                        WHERE pg.physics_group_name = :physicsgroup
                        """ % (self.owner)

    def execute(self, conn, name, transaction = False):
        """
        returns id for a given physics group name
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/PhysicsGroup/GetID. Expects db connection from upper layer.")

        sql = self.sql
        binds = {"physicsgroup": name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["physics_group_id"]
