"""
This module provides Run.SummaryList data access object
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler


class SummaryList(DBFormatter):
    def __init__(self, logger, dbi, owner=""):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT MAX(LUMI_SECTION_NUM) AS MAX_LUMI
        FROM {owner}FILE_LUMIS FL""".format(owner=self.owner)

    def execute(self, conn, dataset="", run_num=-1, transaction=False):
        binds = dict(run_num=run_num)
        wheresql = "WHERE RUN_NUM=:run_num"

        if dataset:
            joins = """JOIN {owner}FILES FS ON FS.FILE_ID=FL.FILE_ID
            JOIN {owner}DATASETS DS ON FS.DATASET_ID=DS.DATASET_ID""".format(owner=self.owner)
            wheresql = "{wheresql} AND DS.DATASET=:dataset".format(wheresql=wheresql)
            sql = "{sql} {joins} {wheresql}".format(sql=self.sql, joins=joins, wheresql=wheresql)
            binds.update(dataset=dataset)
        else:
            sql = "{sql} {wheresql}".format(sql=self.sql, wheresql=wheresql)

        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)

        result = []
        for cursor in cursors:
            result.extend(self.formatCursor(cursor))
        return result
