#!/usr/bin/env python
from WMCore.Database.DBFormatter import DBFormatter

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
	    raise Exception("dbs/dao/Oracle/DoNothing/List expects db connection from upper layer.")
        return []
