#!/usr/bin/env python
"""
This module provides BlockOrigin.List data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class ListBlockOrigin(DBFormatter):
    """
    BlockOrigin  List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT B.BLOCK_NAME, B.OPEN_FOR_WRITING,
B.BLOCK_SIZE, B.FILE_COUNT,
DS.DATASET,
B.ORIGIN_SITE_NAME, B.CREATION_DATE, B.CREATE_BY,
B.LAST_MODIFICATION_DATE, B.LAST_MODIFIED_BY
FROM {owner}BLOCKS B
JOIN {owner}DATASETS DS ON DS.DATASET_ID = B.DATASET_ID """.format(owner=self.owner)

    def execute(self, conn,  origin_site_name="", dataset="", block_name="", transaction = False):
        """
        origin_site_name: T1_US_FNAL_Buffer
        dataset: /a/b/c
        block_name: /a/b/c#d
        """
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed",
                                "Oracle/Block/List.  Expects db connection from upper layer.", self.logger.exception)
        binds = {}
        if origin_site_name:
            wheresql = 'WHERE B.ORIGIN_SITE_NAME = :origin_site_name'
            binds.update(origin_site_name=origin_site_name)

        if dataset:
            if 'wheresql' in locals():
                wheresql += ' AND DS.DATASET = :dataset'
            else:
                wheresql = 'WHERE DS.DATASET = :dataset'
            binds.update(dataset=dataset)

        if block_name:
            if 'wheresql' in locals():
                wheresql += ' AND B.BLOCK_NAME = :block_name'
            else:
                wheresql = 'WHERE B.BLOCK_NAME = :block_name'
            binds.update(block_name=block_name)

        sql = '{sql} {wheresql}'.format(sql=self.sql, wheresql=wheresql)

        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for cursor in cursors:
            result.extend(self.formatCursor(cursor))
        return result
