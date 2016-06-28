#!/usr/bin/env python
"""
This module provides Block.UpdateSiteName data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils


class UpdateSiteName(DBFormatter):
    """
    Block Update Origin Site Name DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE {owner}BLOCKS SET ORIGIN_SITE_NAME = :origin_site_name , LAST_MODIFIED_BY=:myuser,
LAST_MODIFICATION_DATE = :mtime where BLOCK_NAME = :block_name""".format(owner=self.owner)

    def execute(self, conn, block_name, origin_site_name, transaction=False):
        """
        Update origin_site_name for a given block_name
        """
        if not conn:
            dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/Block/UpdateStatus. \
Expects db connection from upper layer.", self.logger.exception)
        binds = {"block_name": block_name, "origin_site_name": origin_site_name, "mtime": dbsUtils().getTime(),
                 "myuser": dbsUtils().getCreateBy()}
        self.dbi.processData(self.sql, binds, conn, transaction)
