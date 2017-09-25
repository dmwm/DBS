#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
from types import GeneratorType
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.logger = logger
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
JOIN %sDATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
""" % ((self.owner,)*5)


    def execute(self, conn, dataset="", block_name="", logical_file_name="",
                release_version="", pset_hash="", app_name="", output_module_label="",
                run_num=-1, origin_site_name="", lumi_list=[], validFileOnly=0, sumOverLumi=0, transaction=False):
        if not conn:
            dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/File/List. Expects db connection from upper layer.", self.logger.exception)
        sql = self.sql_cond
        binds = {}
        sql_sel = self.sql_sel
        run_generator = ''
	lfn_generator = ''
	lumi_generator = ''
        sql_lumi = ''
	#import pdb
	#pdb.set_trace()
        if run_num != -1 :
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
        #WMAgent requires validaFileOnly. YG 1/30/2015
        if int(validFileOnly) == 0:
            sql += """ WHERE F.IS_FILE_VALID <> -1 """
        elif int(validFileOnly) == 1 :
            sql += """ WHERE F.IS_FILE_VALID = 1 
                       AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') """
	else:
	    dbsExceptionHandler("dbsException-invalid-input", "invalid value for validFileOnly.", self.logger.exception)
		
        if block_name:
            if isinstance(block_name, list):
                dbsExceptionHandler('dbsException-invalid-input', 'Input block_name is a list instead of string.', self.logger.exception)
            sql += " AND B.BLOCK_NAME = :block_name"
            binds.update({"block_name":block_name})
        if logical_file_name:
	    if type(logical_file_name) is not list:
		op = ("=", "like")["%" in logical_file_name]
		sql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
		binds.update({"logical_file_name":logical_file_name})
	    if type(logical_file_name) is list:
		ds_generator, binds2 = create_token_generator(logical_file_name)
		binds.update(binds2)
		sql += " AND F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
                lfn_generator = "{ds_generator}".format(ds_generator=ds_generator)
        if dataset:
            if isinstance(dataset, list):
                dbsExceptionHandler('dbsException-invalid-input', 'Input dataset is a list instead of string.', self.logger.exception)
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
            op = ("=", "like")["%" in origin_site_name]
            sql += " AND B.ORIGIN_SITE_NAME %s  :origin_site_name" % op
            binds.update({"origin_site_name":origin_site_name})
        if run_num != -1 and run_num : # elimnate empty list run_num=[]
            run_list=[]
            wheresql_run_list=''
            wheresql_run_range=''
	    wheresql_run_range_ct = 0
	    try:
		run_num = long(run_num)
		sql += " and FL.RUN_NUM = :run_num "
		binds.update({"run_num":run_num})
	    except:
		if isinstance(run_num, basestring):
                    for r in parseRunRange(run_num):
                        if isinstance(r, run_tuple):
                            if r[0] == r[1]:
                                dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.",
					self.logger.exception)
                            if not lumi_list:
                                if wheresql_run_range_ct >0 :
                                    wheresql_run_range += " or "
                                wheresql_run_range += " FL.RUN_NUM between :minrun%s and :maxrun%s " %((wheresql_run_range_ct,)*2)
                                binds.update({"minrun%s"%wheresql_run_range_ct :r[0]})
                                binds.update({"maxrun%s"%wheresql_run_range_ct :r[1]})
                                wheresql_run_range_ct += 1
                            else:
                                dbsExceptionHandler('dbsException-invalid-input', 
				"When lumi_list is given, only one run is allowed.", self.logger.exception)
                        else:
                            dbsExceptionHandler('dbsException-invalid-input', 
			    "Invalid run_num. if run_num input as a string, it has to be converted into a int/long or in format of 'run_min-run_max'. ", self.logger.exception)
		elif type(run_num) is list and len(run_num)==1:
		    try:
			run_num = long(run_num[0])
			sql += " and FL.RUN_NUM = :run_num "
                        binds.update({"run_num":run_num})	
		    except:
			for r in parseRunRange(run_num):
			    if isinstance(r, run_tuple):
				if r[0] == r[1]:
				    dbsExceptionHandler('dbsException-invalid-input', 
				        "DBS run range must be apart at least by 1.", self.logger.exception)
				if not lumi_list:
				    if wheresql_run_range_ct >0 :
                                        wheresql_run_range += " or "
				    wheresql_run_range += " FL.RUN_NUM between :minrun%s and :maxrun%s " %((wheresql_run_range_ct,)*2)
                                    binds.update({"minrun%s"%wheresql_run_range_ct :r[0]})
                                    binds.update({"maxrun%s"%wheresql_run_range_ct :r[1]})
                                    wheresql_run_range_ct += 1	
                                else:
                                    dbsExceptionHandler('dbsException-invalid-input', 
					"When lumi_list is given, only one run is allowed.", self.logger.exception)
			    else:
				dbsExceptionHandler('dbsException-invalid-input', 
				"run_num as a list must be a number or a range str, such as ['10'], [10] or ['1-10']",
				self.logger.exception)	
		else:		
		    for r in parseRunRange(run_num):
			if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):
			    run_list.append(str(r))
			if isinstance(r, run_tuple):
			    if r[0] == r[1]:
				dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.",
				self.logger.exception)
			    if not lumi_list:
				if wheresql_run_range_ct >0 :
				    wheresql_run_range += " or "
                                wheresql_run_range += " FL.RUN_NUM between :minrun%s and :maxrun%s " %((wheresql_run_range_ct,)*2)
				binds.update({"minrun%s"%wheresql_run_range_ct :r[0]})
				binds.update({"maxrun%s"%wheresql_run_range_ct :r[1]})
				wheresql_run_range_ct += 1
			    else:
                                dbsExceptionHandler('dbsException-invalid-input', 
				"When lumi_list is given, only one run is allowed.", self.logger.exception)
            #
            if run_list and not lumi_list:
                wheresql_run_list = " fl.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
                run_generator, run_binds = create_token_generator(run_list)
		#sql =  "{run_generator}".format(run_generator=run_generator) + sql
		binds.update(run_binds)
	    if wheresql_run_range and wheresql_run_list:
		sql += " and (" + wheresql_run_range + " or " +  wheresql_run_list + " )"
            elif wheresql_run_range and not wheresql_run_list:
                sql += " and " + wheresql_run_range
            elif not wheresql_run_range and wheresql_run_list:
                sql += " and "  + wheresql_run_list
        # Make sure when we have a lumi_list, there is only ONE run  -- YG 14/05/2013
        if (lumi_list and len(lumi_list) != 0):
            if len(run_list) > 1:
                dbsExceptionHandler('dbsException-invalid-input', "When lumi_list is given, only one run is allowed.",
					self.logger.exception)         
            sql += " AND FL.LUMI_SECTION_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
            sql_lumi = " FL.LUMI_SECTION_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
            #Do I need to convert lumi_list to be a str list? YG 10/03/13
            #Yes, you do. YG
            lumi_list = map(str, lumi_list)
            lumi_generator, lumi_binds = create_token_generator(lumi_list)
            #sql_sel = "{lumi_generator}".format(lumi_generator=lumi_generator) + sql_sel
            binds.update(lumi_binds)
            #binds["run_num"]=run_list[0]
        #
	if (run_generator and lfn_generator) or (lumi_generator and lfn_generator):
	    dbsExceptionHandler('dbsException-invalid-input2', 
		"cannot supply more than one list (lfn, run_num or lumi) at one query", self.logger.exception,
		"dao/File/list cannot have more than one list (lfn, run_num, lumi) as input pareamters")	
	    # only one with and generators should be named differently for run and lfn.	
	    #sql = run_generator + lfn_generator + lumi_generator  + sql_sel + sql
	else:
            if run_num != -1 and int(sumOverLumi) == 1:                
                sql_sel = sql_sel.replace('F.EVENT_COUNT,', '')
                sql = \
                 'with myfiles as ( ' + sql_sel + sql + """) select mf.* , 
                            (case 
                                when badi.file_id = mc.file_id and badi.run_num=mc.run_num and badi.bid is null then null 
                                else  mc.event_count 
                             end) as event_count 
                     from myfiles mf,   
                          EVENT_COUNT_WITH_LUMI, 
                          ( 
                            select distinct fl.file_id, fl.run_num, null as bid 
                            from %sfile_lumis fl  
                            join myfiles my2 on my2.file_id=fl.file_id and my2.run_num=fl.run_num 
                            where fl.event_count is null 
                         )badi 
                    where mf.file_id= mc.file_id and mf.run_num=mc.run_num  

                    """%self.owner
                if not lumi_list:
                    ent_ct = """
                            (select sum(fl.event_count) as event_count, fl.file_id, fl.run_num 
                            from %sfile_lumis fl 
                            join myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num 
                            group by fl.file_id, fl.run_num) mc 
                        """%self.owner
                    sql = sql.replace('EVENT_COUNT_WITH_LUMI', ent_ct)
                else:
                    ent_ct = lumi_generator  + """ 
                            (select sum(fl.event_count) as event_count, fl.file_id, fl.run_num 
                            from %sfile_lumis fl 
                            join myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num 
                            where sql_lumi  
                            group by fl.file_id, fl.run_num) mc 
                        """%self.owner
                    sql = sql.replace('EVENT_COUNT_WITH_LUMI', ent_ct)
            else:
                sql = run_generator + lfn_generator + lumi_generator  + sql_sel + sql
	self.logger.debug("SQL: " + sql)
        self.logger.debug("***********************")     	
	self.logger.debug(binds)
        try:
            self.logger.debug("******before cursor**********")   
            cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
            self.logger.debug("******after cursor**********")  
        except Exception as e :
            self.logger.error(str(e))
        for i in cursors:
            d = self.formatCursor(i)
            if isinstance(d, list) or isinstance(d, GeneratorType):
                for elem in d:
                    yield elem
            elif d:
                yield d

