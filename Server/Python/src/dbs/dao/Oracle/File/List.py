#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.34 2010/08/23 18:08:41 afaq Exp $"
__version__ = "$Revision: 1.34 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple

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
        self.sql_sel = \
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
"""
        self.sql_cond = \
"""
FROM %sFILES F
JOIN %sFILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID
JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
""" % ((self.owner,)*4)


    def execute(self, conn, dataset="", block_name="", logical_file_name="",
                release_version="", pset_hash="", app_name="", output_module_label="",
                run=-1, origin_site_name="", lumi_list=[], transaction=False):
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed","Oracle/File/List. Expects db connection from upper layer.")
        sql = self.sql_cond
        binds = {}
        sql_sel = self.sql_sel

        if run != -1 :
            sql_sel = sql_sel.replace("SELECT", "SELECT DISTINCT") + " , FL.RUN_NUM  "
            sql += " JOIN %sFILE_LUMIS FL on  FL.FILE_ID=F.FILE_ID " %(self.owner)
        if release_version or pset_hash or app_name or output_module_label :
            sql += """LEFT OUTER JOIN %sFILE_OUTPUT_MOD_CONFIGS FOMC ON FOMC.FILE_ID = F.FILE_ID
                      LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = FOMC.OUTPUT_MOD_CONFIG_ID
                      LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
                      LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
                      LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID
                      """ % ((self.owner,)*5)
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
            op = ("=", "like")["%" in output_module_label]
            sql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" %op
            binds.update({"output_module_label":output_module_label})
        if (origin_site_name):
            op = ("=","like")["%" in origin_site_name]
            sql += " AND B.ORIGIN_SITE_NAME %s  :origin_site_name" % op
            binds.update({"origin_site_name":origin_site_name})
        if run != -1 :
            run_list=[]
            sql_run_list=''
            sql_run_range=''

            for r in parseRunRange(run):
                if isinstance(r, str) or isinstance(r, int):
                    if not sql_run_list:
                        sql_run_list = " FL.RUN_NUM = :run_list "
                    run_list.append(r)
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.")
                    sql_run_range = " FL.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":r[0]})
                    binds.update({"maxrun":r[1]})
            if sql_run_range and len(run_list) >= 1:
                sql += " and (" + sql_run_range + " or " +  sql_run_list + " )"
            elif sql_run_range and not run_list:
                sql += " and " + sql_run_range
            elif not sql_run_range and len(run_list) >= 1:
                sql += " and "  + sql_run_list
            # Any List binding, such as "in :run_list"  or "in :lumi_list" must be the last binding. YG. 22/05/2013
            if len(run_list) == 1:
                binds["run_list"] = run_list[0]
            elif len(run_list) > 1:
                newbinds = []
                for r in run_list:
                    b = {}
                    b.update(binds)
                    b["run_list"] = r
                    newbinds.append(b)
                binds = newbinds
        # Make sure when we have a lumi_list, there is only ONE run  -- YG 14/05/2013
        if (lumi_list and len(lumi_list) != 0):
            sql += " AND FL.LUMI_SECTION_NUM = :lumi_list"
            newbinds=[]
            for alumi in lumi_list:
                cpbinds={}
                cpbinds.update(binds)
                cpbinds["lumi_list"]=alumi
                newbinds.append(cpbinds)
            binds=newbinds
        sql = sql_sel + sql

        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
	return result
