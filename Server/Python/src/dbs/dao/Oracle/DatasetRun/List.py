#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2010/07/09 18:22:13 yuyi Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    DatasetRun List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT DISTINCT FL.RUN_NUM
	FROM %sFILE_LUMIS FL"""% (self.owner)
	
    def execute(self, conn, minrun=-1, maxrun=-1, logical_file_name="", block_name="", dataset="", trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """

	if not conn:
		raise Exception("dbs/dao/Oracle/DatasetRun/List expects db connection from upper layer.")	
        sql = self.sql
        binds = {}
	if logical_file_name and "%" not in logical_file_name:
	    sql += """ inner join FILES on FILES.FILE_ID = FL.FILE_ID
		    WHERE FILES.LOGICAL_FILE_NAME = :logical_file_name"""
	    binds["logical_file_name"] = logical_file_name
	elif block_name and "%" not in block_name:
            sql += """ inner join FILES on FILES.FILE_ID = FL.FILE_ID
		    inner join BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID
		    WHERE BLOCKS.BLOCK_NAME = :block_name """
            binds["block_name"] =  block_name
	elif dataset and "%" not in dataset:
	    sql += """ inner join FILES on FILES.FILE_ID = FL.FILE_ID
	    inner join DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID
	    WHERE DATASETS.DATASET = :dataset """
	    binds["dataset"] = dataset
	else:
	    pass
	if minrun > 0:
	    if "WHERE" in sql:
		sql += " and FL.RUN_NUM >= :min_run"
	    else:
		sql += " WHERE FL.RUN_NUM >= :min_run"
	    binds["min_run"] = minrun
	if maxrun > 0:
		if "WHERE" in sql:
		    sql += " AND FL.RUN_NUM <= :max_run"
		else:
		    sql += " where FL.RUN_NUM <= :max_run"
		binds["max_run"] = maxrun
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
