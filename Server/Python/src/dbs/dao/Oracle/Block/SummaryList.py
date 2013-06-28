"""
This module provides Block.SummaryList data access object
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler


class SummaryList(DBFormatter):
    def __init__(self, logger, dbi, owner=""):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.block_join = "JOIN {owner}BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID".format(owner=self.owner)
        self.dataset_join = "JOIN {owner}DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID".format(owner=self.owner)

    def execute(self, conn, block_name="", dataset="", transaction=False):
        binds = {}

        if dataset:
            where_clause = "WHERE DS.dataset=:dataset"
            sql = """SELECT (
            SELECT SUM(BS.BLOCK_SIZE)
            FROM {owner}BLOCKS BS {dataset_join} {where_clause}
            ) AS FILE_SIZE,
            (
            SELECT SUM(BS.FILE_COUNT)
            FROM {owner} BLOCKS BS {dataset_join} {where_clause}
            ) AS NUM_FILE,
            (
            SELECT SUM(FS.EVENT_COUNT)
            FROM {owner}FILES FS {block_join} {dataset_join} {where_clause}
            ) AS NUM_EVENT
            FROM DUAL""".format(owner=self.owner, where_clause=where_clause, dataset_join=self.dataset_join,
                                block_join=self.block_join)
            binds.update(dataset=dataset)

        else:
            # Oracle IN only supports a maximum of 1,000 values
            # (ORA-01795: maximum number of expressions in a list is 1000)
            block_clause = "BS.BLOCK_NAME IN ("
            for counter, this_block_name in enumerate(block_name):
                block_label = 'block_%s' % counter
                binds.update({block_label: this_block_name})
                block_clause += ":%s, " % block_label
            block_clause = block_clause[:-2]+")"#remove last comma and space in the list above
            where_clause = "WHERE {block_clause}".format(block_clause=block_clause)

            sql = """SELECT (
            SELECT SUM(BS.BLOCK_SIZE)
            FROM {owner}BLOCKS BS {where_clause}
            ) AS FILE_SIZE,
            (
            SELECT SUM(BS.FILE_COUNT)
            FROM {owner} BLOCKS BS {where_clause}
            ) AS NUM_FILE,
            (
            SELECT SUM(FS.EVENT_COUNT)
            FROM {owner}FILES FS {block_join} {where_clause}
            ) AS NUM_EVENT
            FROM DUAL""".format(owner=self.owner, where_clause=where_clause, block_join=self.block_join)

        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)

        result = []
        for cursor in cursors:
            result.extend(self.formatCursor(cursor))
        return result
