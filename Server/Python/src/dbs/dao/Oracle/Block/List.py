#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.24 2010/08/09 20:02:46 yuyi Exp $"
__version__ = "$Revision: 1.24 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface

class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
	"""
	Add schema owner and sql.
	"""
	DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
    """
SELECT B.BLOCK_ID, B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
        B.BLOCK_SIZE, B.FILE_COUNT,
        B.DATASET_ID, DS.DATASET,
        B.ORIGIN_SITE_NAME, B.CREATION_DATE, B.CREATE_BY
FROM %sBLOCKS B
JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID
    """ % ((self.owner,)*2)
#
    def execute(self, conn, dataset="", block_name="", origin_site_name="", logical_file_name="", run_num=-1, transaction = False):
	"""
	dataset: /a/b/c
	block: /a/b/c#d
	"""	
	if not conn:
	    raise Exception("dbs/dao/Oarcle/Block/List expects db connection from upper layer.")
	sql = self.sql
	binds = {}

	if logical_file_name and logical_file_name != "%":
	    op = ("=", "like")["%" in logical_file_name] 
	    sql += " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID" %(self.owner)
	    if run_num and run_num !=-1:
		sql += " JOIN %s FILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID" %(self.owner)
	    sql += " WHERE LOGICAL_FILE_NAME %s :logical_file_name" % op
	    binds.update( logical_file_name = logical_file_name)
	    if run_num and run_num !=-1:
		sql += " AND RUN_NUM = :run_num"
		binds.update(run_num = run_num)
	    if  block_name and  block_name !="%":
		op = ("=", "like")["%" in block_name]
		sql += " AND B.BLOCK_NAME %s :block_name" % op
		binds.update({"block_name":block_name})
		if dataset and dataset !="%": 
		    op = ("=", "like")["%" in dataset]
		    sql += " AND DS.DATASET %s :dataset" %op
		    binds.update(dataset=dataset)
		if origin_site_name and  origin_site_name != "%":
		    op = ("=", "like")["%" in origin_site_name]
		    sql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" %op
		    binds.update(origin_site_name = origin_site_name)	
	    elif dataset and dataset !="%": 
		op = ("=", "like")["%" in dataset]
		sql += " AND DS.DATASET %s :dataset" %op
		binds.update(dataset=dataset)
		if origin_site_name and  origin_site_name != "%":
		    op = ("=", "like")["%" in origin_site_name]
		    sql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" %op
		    binds.update(origin_site_name = origin_site_name)
	    elif origin_site_name and  origin_site_name != "%": 
		op = ("=", "like")["%" in origin_site_name] 
		sql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" %op
		binds.update(origin_site_name = origin_site_name)
	elif block_name and  block_name !="%":
	    if run_num and run_num !=-1:
		sql += """ JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID
		            JOIN %s FILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID
			""" %((self.owner,)*2)
	    op = ("=", "like")["%" in block_name]
	    sql += " WHERE B.BLOCK_NAME %s :block_name" % op
	    binds.update({"block_name":block_name}) 
	    if run_num and run_num !=-1:
                sql += " AND RUN_NUM = :run_num"
                binds.update(run_num = run_num)
	    if dataset and dataset !="%": 
		op = ("=", "like")["%" in dataset]
		sql += " AND DS.DATASET %s :dataset" %op
		binds.update(dataset=dataset)
	    if origin_site_name and  origin_site_name != "%":
		op = ("=", "like")["%" in origin_site_name]
		sql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" %op
		binds.update(origin_site_name = origin_site_name)	
	elif dataset and dataset !="%":
	    if run_num and run_num !=-1:
                sql += """ JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID 
		            JOIN %s FILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID
			""" %((self.owner,)*2)
	    op = ("=", "like")["%" in dataset] 
	    sql += " WHERE DS.DATASET %s :dataset" %op
	    binds.update(dataset=dataset) 
	    if run_num and run_num !=-1:
		sql += " AND RUN_NUM = :run_num"
                binds.update(run_num = run_num)
	    if origin_site_name and  origin_site_name != "%": 
		op = ("=", "like")["%" in origin_site_name] 
		sql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" %op
		binds.update(origin_site_name = origin_site_name)

	if run_num and run_num !=-1:
	    sql = sql.replace("SELECT", "SELECT DISTINCT")

	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
	result = self.formatCursor(cursors[0])
	return result
