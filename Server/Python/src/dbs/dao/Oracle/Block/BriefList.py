#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: BriefList.py,v 1.1 2010/08/01 19:03:30 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter


class BriefList(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
	"""
	Add schema owner and sql.
	"""
	DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """ SELECT  B.BLOCK_NAME FROM %sBLOCKS B """ % self.owner

    def execute(self, conn, dataset="", block_name="", origin_site_name="", logical_file_name="", run_num=-1, transaction = False):
	"""
	dataset: /a/b/c
	block: /a/b/c#d
	"""	

	binds = {}

	basesql = self.sql
	joinsql = ""
	wheresql = ""

	if logical_file_name and logical_file_name != "%":
	    joinsql +=  " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID " %(self.owner)
	    op =  ("=", "like")["%" in logical_file_name]
	    wheresql +=  " WHERE LOGICAL_FILE_NAME %s :logical_file_name" % op
	    binds.update( logical_file_name = logical_file_name )

	if  block_name and  block_name !="%": 
	    andorwhere = ("WHERE", "AND")[bool(wheresql)]
	    op =  ("=", "like")["%" in block_name]
	    wheresql +=  " %s B.BLOCK_NAME %s :block_name" % ((andorwhere, op))
	    binds.update( block_name = block_name )

	if dataset and dataset !="%":
	    joinsql += "JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID"  % (self.owner)
	    andorwhere = ("WHERE", "AND")[bool(wheresql)]
	    op = ("=", "like")["%" in dataset]
	    wheresql += " %s DS.DATASET %s :dataset" % ((andorwhere, op))
	    binds.update(dataset=dataset)

	assert wheresql, 'Either logical_file_name, block_name or dataset parameter must be provided'

	if origin_site_name and  origin_site_name != "%": 
	    op = ("=", "like")["%" in origin_site_name]
	    wheresql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name" % op
	    binds.update(origin_site_name = origin_site_name)	

	if run_num !=-1:
	    if not logical_file_name:
		joinsql +=  " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID" %(self.owner)
	    joinsql += " JOIN %s FILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID" %(self.owner)
	    wheresql += " AND FLM.RUN_NUM = :run_num"
	    basesql = basesql.replace("SELECT", "SELECT DISTINCT")
	    binds.update(run_num = run_num)

	sql = " ".join((basesql, joinsql, wheresql))  
		
	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
	result = self.formatCursor(cursors[0])
	return result
