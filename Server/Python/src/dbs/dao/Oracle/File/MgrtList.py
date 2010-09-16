#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__vision__ = "$Id: MgrtList.py,v 1.1 2010/08/06 18:30:23 yuyi Exp $"
__reversion__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class MgrtList(DBFormatter):
    """
    File List DAO class for migration.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = \
"""
SELECT F.LOGICAL_FILE_NAME, F.IS_FILE_VALID, 
       FT.FILE_TYPE,
       F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE,  
       F.BRANCH_HASH_ID, F.ADLER32, F.MD5, 
       F.AUTO_CROSS_SECTION,
       F.CREATION_DATE, F.CREATE_BY, 
       F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
FROM %sFILES F 
JOIN %sFILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID 
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
""" % ((self.owner,)*3)


    def execute(self, conn, block_name="", logical_file_name="",
	    maxrun=-1, minrun=-1, origin_site_name="", lumi_list=[], transaction=False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/List expects db connection from upper layer.")
	sql = self.sql 
        binds = {}
	if (minrun and minrun != -1) and (maxrun and maxrun != -1):
	    sql = sql.replace("SELECT", "SELECT DISTINCT")
	    sql += "JOIN %sFILE_LUMIS FL on  FL.FILE_ID=F.FILE_ID " %(self.owner)
	#FIXME : the status check should only be done with normal/super user
        #sql += """WHERE F.IS_FILE_VALID = 1"""
        # for the time being lests list all files
        sql += """WHERE F.IS_FILE_VALID <> -1 """
        if block_name:
            sql += " AND B.BLOCK_NAME = :block_name"
            binds.update({"block_name":block_name})
        if logical_file_name:
	    op = ("=", "like")["%" in logical_file_name] 
            sql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
            binds.update({"logical_file_name":logical_file_name})
	if (minrun and minrun != -1 and maxrun and maxrun != -1):
	    sql += " AND FL.RUN_NUM between :minrun and :maxrun " 
	    binds.update({"minrun":minrun})
	    binds.update({"maxrun":maxrun})
	if (lumi_list and len(lumi_list) != 0):
	    sql += "AND FL.LUMI_SECTION_NUM in :lumi_list"
	    binds.update({"lumi_list":lumi_list})
	if (origin_site_name):
	    op = ("=","like")["%" in origin_site_name]
    	    sql += "AND B.ORIGIN_SITE_NAME %s  :origin_site_name" % op 
	    binds.update({"origin_site_name":origin_site_name})
	#    
	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	if len(cursors) != 1 :
	    raise Exception("File does not exist.")

	result = self.formatCursor(cursors[0])
	return result
