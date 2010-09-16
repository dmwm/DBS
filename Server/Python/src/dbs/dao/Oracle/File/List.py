#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.22 2010/03/10 17:11:17 akhukhun Exp $"
__version__ = "$Revision: 1.22 $"

from WMCore.Database.DBFormatter import DBFormatter


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
	#all listFile APIs should return the same data structure defined by self.sql
        self.sql = \
"""
SELECT F.FILE_ID, F.LOGICAL_FILE_NAME, F.IS_FILE_VALID, 
        F.DATASET_ID, D.DATASET,
        F.BLOCK_ID, B.BLOCK_NAME, 
        F.FILE_TYPE_ID, FT.FILE_TYPE,
        F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE,  
        F.BRANCH_HASH_ID, F.ADLER32, F.MD5, 
        F.AUTO_CROSS_SECTION,
        F.CREATION_DATE, F.CREATE_BY, 
        F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
FROM %sFILES F 
JOIN %sFILE_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID 
JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID 
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
""" % ((self.owner,)*4)


    def execute(self, conn, dataset="", block_name="", logical_file_name="", 
	    release_version="", pset_hash="", app_name="", output_module_label="",
	    transaction=False):

        """
        dataset: /a/b/c
        block_name: /a/b/c#d
        logical_file_name: string
        """	
        if not conn:
            raise Exception("dbs/dao/Oracle/File/List expects db connection from up layer.")
        sql = self.sql 
        binds = {}
        op = ("=","like")["%" in logical_file_name]
            
	if release_version or pset_hash or app_name or output_module_label :
	    sql += """LEFT OUTER JOIN %sFILE_OUTPUT_MOD_CONFIGS FOMC ON FOMC.FILE_ID = D.DATASET_ID
			LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = FOMC.OUTPUT_MOD_CONFIG_ID
			LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
			LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
			LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID
			""" % ((self.owner,)*5)
	sql += """WHERE F.IS_FILE_VALID = 1"""
        if block_name:
            sql += " AND B.BLOCK_NAME = :block_name"
            binds.update({"block_name":block_name})
        if logical_file_name:
            sql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
            binds.update({"logical_file_name":logical_file_name})
        if dataset: 
            sql += " AND D.DATASET = :dataset"
            binds.update({"dataset":dataset})
        if release_version:
	    op = ("=", "like")["%" in release_version]
            sql += " AND RV.RELEASE_VERSION %s :release_version" % op
            binds.update({"release_version":release_version})
        if pset_hash:
	    op = ("=", "like")["%" in pset_hash]
            sql += " AND PSH.PSET_HASH %s :pset_hash" % op
            binds.update({"pset_hash" :pset_hash})
        if app_name:
	    op = ("=", "like")["%" in app_name]
            sql += " AND AEX.APP_NAME %s :app_name" % op
            binds.update({"app_name":  app_name})
        if output_module_label:
            sql += " AND OMC.OUTPUT_MODULE_LABEL  = :output_module_label" 
            binds.update({"output_module_label":output_module_label})
	
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "File does not exist"
		
        result = self.formatCursor(cursors[0])
        return result 

	def executeByRun(self, conn, maxrun, minrun, blockName, transaction=False):
	    """
	     Select a list of Files with in minrun and maxrun. The maxrun has to be defined. 
	     conn has to be passed into the dao object.
	    """
	    if conn:
		raise Exception("No connection to DB")
	    
	    binds = {}
	    sql = self.sql + "JOIN %sFILE_LUMIS FL on  FL.FILE_ID=F.FILE_ID \
				WHERE F.IS_FILE_VALID = 1 \
				and FL.RUN_NUM between :minrun and :maxrun \
				and B.BLOCK_NAME like :blockName " %(self.owner)
	    binds.update({"minrun":minrun})
	    binds.update({"maxrun":maxrun})
	    binds.update({"blockName":blockName})
	    cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	    assert len(cursors) == 1, "File does not exist"
	    result = self.formatCursor(cursors[0])
	    return result

	def executeBySite(self, conn, originSite, blockName, transaction=False):
	    """
	    Select a list of Files from a dataset/block within the originSite. We treat dataset as 
             block by using like in the query since block_name=datset_name + #UUID.
	    """ 
	    if conn:
		raise Exception("No connection to DB")
	    binds = {}
    	    sql = self.sql + " JOIN %sSITES ST on ST.SITE_ID = B.ORIGIN_SITE \
	                       WHERE F.IS_FILE_VALID = 1 and \
			       ST.SITE_NAME = :originSite and B.BLOCK_NAME like :blockName" %(self.owner)
	    binds.update({"originSite":originSite})
	    binds.update({"blockName":blockName})
	    cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	    assert len(cursors) == 1, "File does not exist"

	    result = self.formatCursor(cursors[0])
	    return result


