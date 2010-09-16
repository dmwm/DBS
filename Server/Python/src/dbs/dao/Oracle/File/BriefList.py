#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: BriefList.py,v 1.4 2010/08/30 16:21:44 afaq Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class BriefList(DBFormatter):
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
        self.sql = " SELECT F.LOGICAL_FILE_NAME FROM %sFILES F  " % self.owner

    def execute(self, conn, dataset="", block_name="", logical_file_name="",
            release_version="", pset_hash="", app_name="", output_module_label="",
	    maxrun=-1, minrun=-1, origin_site_name="", lumi_list=[], transaction=False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/List expects db connection from upper layer.")
        binds = {}
	basesql = self.sql
	joinsql = ""
        # for the time being lests list all files
	wheresql = "WHERE F.IS_FILE_VALID <> -1"

        if logical_file_name:
	    op = ("=", "like")["%" in logical_file_name] 
            wheresql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
            binds.update({"logical_file_name":logical_file_name})

        if block_name:
	    joinsql += " JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID" % (self.owner)
            wheresql += " AND B.BLOCK_NAME = :block_name"
            binds.update({"block_name":block_name})

        if dataset: 
	    joinsql += " JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID " % (self.owner)
            wheresql += " AND D.DATASET = :dataset"
            binds.update({"dataset":dataset})


	if release_version or pset_hash or app_name or output_module_label:
	    joinsql += """
		LEFT OUTER JOIN %sFILE_OUTPUT_MOD_CONFIGS FOMC ON FOMC.FILE_ID = F.FILE_ID
		LEFT OUTER JOIN %sOUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID = FOMC.OUTPUT_MOD_CONFIG_ID 
	    """ % ((self.owner,)*2)
	    
	if release_version:
	    joinsql += " LEFT OUTER JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID" % (self.owner)
	    op = ("=", "like")["%" in release_version]
	    wheresql += " AND RV.RELEASE_VERSION %s :release_version" % op
	    binds.update(release_version=release_version)

        if pset_hash:
	    joinsql += " LEFT OUTER JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID" % (self.owner)
	    op = ("=", "like")["%" in pset_hash]
	    wheresql += " AND PSH.PSET_HASH %s :pset_hash" % op
	    binds.update(pset_hash = pset_hash)

        if app_name:
	    joinsql += " LEFT OUTER JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID" % (self.owner)
	    op = ("=", "like")["%" in app_name]
	    wheresql += " AND AEX.APP_NAME %s :app_name" % op
	    binds.update(app_name = app_name)

        if output_module_label:
	    op = ("=", "like")["%" in output_module_label]
	    wheresql += " AND OMC.OUTPUT_MODULE_LABEL  %s :output_module_label" % op
	    binds.update(output_module_label=output_module_label)

	
	if (minrun and minrun != -1 and maxrun and maxrun != -1):
	    basesql = basesql.replace("SELECT", "SELECT DISTINCT")
	    joinsql += " JOIN %sFILE_LUMIS FL on  FL.FILE_ID=F.FILE_ID " %(self.owner)
	    wheresql += " AND FL.RUN_NUM between :minrun and :maxrun " 
	    binds.update({"minrun":minrun})
	    binds.update({"maxrun":maxrun})

	if (origin_site_name):
	    if not block_name:
		joinsql += " JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID" % (self.owner)
	    op = ("=","like")["%" in origin_site_name]
    	    wheresql += " AND B.ORIGIN_SITE_NAME %s  :origin_site_name" % op 
	    binds.update({"origin_site_name":origin_site_name})

        # KEEP lumi_list as the LAST CHECK in this DAO, this is a MUST ---  ANZAR 08/23/2010
        if (lumi_list and len(lumi_list) != 0):
            wheresql += " AND FL.LUMI_SECTION_NUM in ( "
	    counter=0
            for alumi in lumi_list:
		if counter>0:
		    wheresql += ","
		wheresql += ":lumi_b%s" %counter
		binds.update({"lumi_b%s" %counter : alumi})
		counter+=1
	    wheresql += ")"

	sql = " ".join((basesql, joinsql, wheresql))
	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	#if len(cursors) != 1 :
	#    raise Exception("File does not exist.")
	result = self.formatCursor(cursors[0])
	return result
