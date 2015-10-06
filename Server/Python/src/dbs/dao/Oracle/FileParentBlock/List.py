#!/usr/bin/env python
"""
This module provides FileParentBlock.List data access object.

Given the ID of a File, returns a LIST of the dicts containing IDs 
[{block_id, dataset_id},....] of the Parent BLOCK of the 
Block containing THIS file.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT B.BLOCK_ID as BLOCK_ID, B.DATASET_ID as DATASET_ID FROM %sBLOCKS B JOIN %sFILES FL ON FL.BLOCK_ID=B.BLOCK_ID LEFT OUTER JOIN %sFILE_PARENTS FP ON FP.PARENT_FILE_ID = FL.FILE_ID WHERE FP.THIS_FILE_ID IN ( """% ((self.owner,)*3)

    def execute(self, conn, file_id_list, transaction=False):
	"""
	file_id_list : file_id_list 
	"""
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileParentBlock/List. Expects db connection from upper layer.")

	sql=self.sql
	binds={}
	if file_id_list:
	    count=0
	    for an_id in file_id_list:
		if count > 0: sql += ", "
		sql += ":file_id_%s" %count
		binds.update({"file_id_%s" %count : an_id})
		count+=1
	    sql += ")"
	else:
            dbsExceptionHandler('dbsException-invalid-input', "Oracle/FileParentBlock/List. this_file_id not provided")
            
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	return plist
