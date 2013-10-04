#!/usr/bin/env python
"""
This module provides FileParent.ListChild data access object.
"""
__revision__ = "$Id: ListChild.py,v 1.2 2010/06/23 21:21:24 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSDaoTools import create_token_generator


class ListChild(DBFormatter):
    """
    FileParent List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """
        SELECT CF.LOGICAL_FILE_NAME child_logical_file_name,
        CF.FILE_ID child_file_id,
        F.LOGICAL_FILE_NAME
        FROM {owner}FILES CF
        JOIN {owner}FILE_PARENTS FP ON FP.THIS_FILE_ID = CF.FILE_ID
        JOIN {owner}FILES F ON  F.FILE_ID = FP.PARENT_FILE_ID
        """.format(owner=self.owner)

    def execute(self, conn, logical_file_name, block_name, block_id, transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed",
                                "Oracle/FileParent/ListChild. Expects db connection from upper layer.")

        binds = {}
        sql = ''

        if logical_file_name:
            if isinstance(logical_file_name, str):
                wheresql = "WHERE F.LOGICAL_FILE_NAME = :logical_file_name"
                binds = {"logical_file_name": logical_file_name}
                sql = "{sql} {wheresql}".format(sql=self.sql, wheresql=wheresql)
            elif isinstance(logical_file_name, list):
                wheresql = "WHERE F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
                lfn_generator, binds = create_token_generator(logical_file_name)
                sql = "{lfn_generator} {sql} {wheresql}".format(lfn_generator=lfn_generator, sql=self.sql,
                                                                wheresql=wheresql)
        elif block_name:
            joins = "JOIN {owner}BLOCKS B on B.BLOCK_ID = F.BLOCK_ID".format(owner=self.owner)
            wheresql = "WHERE B.BLOCK_NAME = :block_name"
            binds = {"block_name": block_name}
            sql = "{sql} {joins} {wheresql}".format(sql=self.sql, joins=joins, wheresql=wheresql)
        elif block_id:
            wheresql = "WHERE F.BLOCK_ID = :block_id"
            binds = {"block_id": block_id}
            sql = "{sql} {wheresql}".format(sql=self.sql, wheresql=wheresql)
        else:
            dbsExceptionHandler('dbsException-invalid-input', "Logical_file_names is required for listChild dao.")

        cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
