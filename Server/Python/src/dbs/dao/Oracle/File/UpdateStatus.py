#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.DBSDaoTools import create_lfn_generator

class UpdateStatus(DBFormatter):

    """
    File Update Status DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE {owner}FILES F SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate,
        IS_FILE_VALID = :is_file_valid
        """.format(owner=self.owner)

    def execute(self, conn, logical_file_name, is_file_valid, lost, transaction=False):
        """
        for a given file or a list of files
        """
        if not conn:
           dbsExceptionHandler("dbsException-db-conn-failed",
                               "Oracle/File/UpdateStatus. Expects db connection from upper layer.")

        binds = dict(myuser=dbsUtils().getCreateBy(),
                     mydate=dbsUtils().getTime(),
                     is_file_valid=is_file_valid)

        if isinstance(logical_file_name, list):
            lfn_generator, lfn_binds = create_lfn_generator(logical_file_name)
            ###with clause - subquery factory does only work with select statements, therefore lfn_generator
            ###has to be place in front of the SELECT statement in the WHERE clause
            ###http://asktom.oracle.com/pls/asktom/f?p=100:11:::::P11_QUESTION_ID:8120272301765
            wheresql = """WHERE F.LOGICAL_FILE_NAME in ({lfn_generator} SELECT LOGICAL_FILE_NAME FROM LFN_GENERATOR)
            """.format(lfn_generator=lfn_generator)
            binds.update(lfn_binds)
        else:
            wheresql = "where F.LOGICAL_FILE_NAME=:logical_file_name"
            binds.update(logical_file_name=logical_file_name)

        if lost:
            sql = "{sql}, file_size=0 {wheresql}".format(sql=self.sql,
                                                         wheresql=wheresql)
        else:
            sql = "{sql} {wheresql}".format(sql=self.sql,
                                            wheresql=wheresql)

        self.dbi.processData(sql, binds, conn, transaction)
