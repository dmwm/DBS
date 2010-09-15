#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.15 2010/01/27 17:27:21 afaq Exp $"
__version__ = "$Revision: 1.15 $"

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
        self.owner = ("","%s." % owner)[bool(owner)]
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
WHERE F.IS_FILE_VALID = 1
""" % ((self.owner,)*4)


    def execute(self, dataset="", block_name="", logical_file_name="", 
	    release_version="", pset_hash="", app_name="", output_module_label="",
	    conn=None):
        """
        dataset: /a/b/c
        block_name: /a/b/c#d
        logical_file_name: string
        """	
        if not conn:
            conn = self.dbi.connection()
        sql = self.sql
        binds = {}
        op = ("=","like")["%" in logical_file_name]
            
	if release_version or pset_hash or app_name or output_module_label :
	    sql += """LEFT OUTER JOIN %sFILE_OUTPUT_MOD_CONFIGS FOMC ON FOMC.FILE_ID = D.DATASET_ID
			LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = FOMC.OUTPUT_MOD_CONFIG_ID
			LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
			LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
			LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID""" % ((self.owner,)*5)
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
            sql += " AND RV.RELEASE_VERSION = :release_version"
            binds.update({"release_version":release_version})
        if pset_hash:
            sql += " AND PSH.PSET_HASH = :pset_hash"
            binds.update({"pset_hash" :pset_hash})
        if app_name:
            sql += " AND AEX.APP_NAME = :app_name"
            binds.update({"app_name":  app_name})
        if output_module_label:
            sql += " AND OMC.OUTPUT_MODULE_LABEL  = :output_module_label" 
            binds.update({"output_module_label":output_module_label})
	if not dataset and not block_name and not logical_file_name and not release_version \
				    and not pset_hash and not app_name and not output_module_label:
            raise Exception("Either dataset or block must be provided")
        
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result 

