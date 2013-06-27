#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.5 2010/06/23 21:21:23 afaq Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils


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

    def execute(self, conn, logical_file_names, is_file_valid, lost, transaction=False):
        """
        for a given file or a list of files
        """
        if not conn:
           dbsExceptionHandler("dbsException-db-conn-failed","Oracle/File/UpdateStatus. Expects db connection from upper layer.")
        binds = {}
        bindlist = []

        op = ("=", "in")[type(logical_file_names) is list]
        if lost == 1:
            self.sql = """UPDATE %sFILES SET LAST_MODIFIED_BY=:myuser, LAST_MODIFICATION_DATE=:mydate, IS_FILE_VALID = :is_file_valid,
            file_size=0 where LOGICAL_FILE_NAME %s :logical_file_names""" %(self.owner,op)
        else:
            self.sql = """UPDATE %sFILES SET LAST_MODIFIED_BY=:myuser, LAST_MODIFICATION_DATE=:mydate, IS_FILE_VALID = :is_file_valid
            where LOGICAL_FILE_NAME %s :logical_file_names""" %  (self.owner, op)
        if op == '=':
            binds = {"is_file_valid" : is_file_valid, "logical_file_names": logical_file_names, "myuser":dbsUtils().getCreateBy(), "mydate": dbsUtils().getTime()}
        else:
            for f in logical_file_names:
                binds = {"is_file_valid" : is_file_valid, "logical_file_names":f, "myuser":dbsUtils().getCreateBy(),
                         "mydate": dbsUtils().getTime()}
                bindlist.append(binds)
        if bindlist:
            binds=bindlist

        result = self.dbi.processData(self.sql, binds, conn, transaction)
