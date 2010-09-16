#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.19 2010/05/19 16:18:39 yuyi Exp $"
__version__ = "$Revision: 1.19 $"

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
        self.sql1 = \
    """
SELECT B.BLOCK_ID, B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
        B.BLOCK_SIZE, B.FILE_COUNT,
        B.DATASET_ID, DS.DATASET,
        B.ORIGIN_SITE_NAME
FROM %sBLOCKS B
JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID
    """ % ((self.owner,)*2)
#
    def execute(self, conn, dataset="", block_name="", original_site_name="", logical_file_name="", transaction = False):
	"""
	dataset: /a/b/c
	block: /a/b/c#d
	"""	
	if not conn:
	    raise Exception("dbs/dao/Oarcle/Block/List expects db connection from up layer.")
	sql1 = self.sql1
	binds = {}
	if logical_file_name and logical_file_name != "%":
	    op = ("=", "like")["%" in logical_file_name] 
	    sql1 += " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID" %(self.owner)
	    sql1 += " WHERE LOGICAL_FILE_NAME %s :logical_file_name" % op
	    binds.update( logical_file_name = logical_file_name)
	    if  block_name and  block_name !="%":
		op = ("=", "like")["%" in block_name]
		sql1 += " AND B.BLOCK_NAME %s :block_name" % op
		binds.update({"block_name":block_name})
		if dataset and dataset !="%": 
		    op = ("=", "like")["%" in dataset]
		    sql1 += " AND DS.DATASET %s :dataset" %op
		    binds.update(dataset=dataset)
		if original_site_name and  original_site_name != "%":
		    op = ("=", "like")["%" in original_site_name]
		    sql1 += " AND B.ORIGIN_SITE_NAME %s :original_site_name" %op
		    binds.update(original_site_name = original_site_name)	
	    elif dataset and dataset !="%": 
		op = ("=", "like")["%" in dataset]
		sql1 += " AND DS.DATASET %s :dataset" %op
		binds.update(dataset=dataset)
		if original_site_name and  original_site_name != "%":
		    op = ("=", "like")["%" in original_site_name]
		    sql1 += " AND B.ORIGIN_SITE_NAME %s :original_site_name" %op
		    binds.update(original_site_name = original_site_name)
	    elif original_site_name and  original_site_name != "%": 
		op = ("=", "like")["%" in original_site_name] 
		sql1 += " AND B.ORIGIN_SITE_NAME %s :original_site_name" %op
		binds.update(original_site_name = original_site_name)
	elif block_name and  block_name !="%":
	    op = ("=", "like")["%" in block_name]
	    sql1 += " WHERE B.BLOCK_NAME %s :block_name" % op
	    binds.update({"block_name":block_name}) 
	    if dataset and dataset !="%": 
		op = ("=", "like")["%" in dataset]
		sql1 += " AND DS.DATASET %s :dataset" %op
		binds.update(dataset=dataset)
	    if original_site_name and  original_site_name != "%":
		op = ("=", "like")["%" in original_site_name]
		sql1 += " AND B.ORIGIN_SITE_NAME %s :original_site_name" %op
		binds.update(original_site_name = original_site_name)	
	elif dataset and dataset !="%":
	    op = ("=", "like")["%" in dataset] 
	    sql1 += " WHERE DS.DATASET %s :dataset" %op
	    binds.update(dataset=dataset) 
	    if original_site_name and  original_site_name != "%": 
		op = ("=", "like")["%" in original_site_name] 
		sql1 += " AND B.ORIGIN_SITE_NAME %s :original_site_name" %op
		binds.update(original_site_name = original_site_name)
	#print "sql1=%s" %sql1
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql1, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
	result = self.formatCursor(cursors[0])
	return result
