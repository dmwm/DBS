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
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
    """
SELECT B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
        B.BLOCK_SIZE, B.FILE_COUNT,
        DS.DATASET,
        B.ORIGIN_SITE_NAME, B.CREATION_DATE, B.CREATE_BY,
        B.LAST_MODIFICATION_DATE, B.LAST_MODIFIED_BY
FROM %sBLOCKS B
JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID 
WHERE B.ORIGIN_SITE_NAME = :origin_site_name
    """ % ((self.owner,)*2)
#
    def execute(self, conn,  origin_site_name="", dataset="", transaction = False):
	"""
        origin_site_name: T1_US_FNAL_Buffer
	dataset: /a/b/c
	"""	
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Block/List.  Expects db connection from upper layer.")
	sql = self.sql
	binds = {}
        binds.update(origin_site_name = origin_site_name)

        if dataset:
            sql += " and DS.DATASET = :dataset"
            binds.update(dataset=dataset)

        #print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block/dataset does not exist"
	result = self.formatCursor(cursors[0])
	return result
