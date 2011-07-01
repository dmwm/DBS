#!/usr/bin/env python
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    DoNothing List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = ""

    def execute(self, conn, transaction = False):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/DoNothing/List. Expects db connection from upper layer.")

        return []
