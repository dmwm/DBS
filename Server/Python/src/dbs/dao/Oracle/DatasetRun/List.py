#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2010/07/09 18:22:13 yuyi Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

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
	
    def execute(self, conn, run_num=-1, logical_file_name="", block_name="", dataset="", trans=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/DatasetRun/List. Expects db connection from upper layer.")

        sql = self.sql
        binds = {}
	if logical_file_name and "%" not in logical_file_name:
	    sql += """ inner join %sFILES FILES on FILES.FILE_ID = FL.FILE_ID
		    WHERE FILES.LOGICAL_FILE_NAME = :logical_file_name"""%(self.owner)
	    binds["logical_file_name"] = logical_file_name
	elif block_name and "%" not in block_name:
            sql += """ inner join %sFILES FILES on FILES.FILE_ID = FL.FILE_ID
		    inner join %sBLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID
		    WHERE BLOCKS.BLOCK_NAME = :block_name """%(self.owner, self.owner)
            binds["block_name"] =  block_name
	elif dataset and "%" not in dataset:
	    sql += """ inner join %sFILES FILES on FILES.FILE_ID = FL.FILE_ID
	    inner join %sDATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID
	    WHERE DATASETS.DATASET = :dataset """%(self.owner, self.owner)
	    binds["dataset"] = dataset
	else:
	    pass
        
	if run_num != -1:
            andorwhere = ("WHERE", "AND")["WHERE" in sql]
            run_list = []
            wheresql_run_list = ''
            wheresql_run_range = ''
            #
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):
                    run_list.append(str(r))
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run_num range must be apart at least by 1.")
                    wheresql_run_range = " FL.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":r[0]})
                    binds.update({"maxrun":r[1]})
            # 
            if run_list:
                wheresql_run_list = " fl.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
                run_generator, run_binds = create_token_generator(run_list)
                sql =  "{run_generator}".format(run_generator=run_generator) + sql
                binds.update(run_binds)

            if wheresql_run_range and wheresql_run_list:
                sql += " %s (" %andorwhere    + wheresql_run_range + " or " +  wheresql_run_list + " )"
            elif wheresql_run_range and not wheresql_run_list:
                sql += " %s " %andorwhere  + wheresql_run_range
            elif not wheresql_run_range and wheresql_run_list:
                sql += " %s " %andorwhere  + wheresql_run_list
        #self.logger.debug(sql)
	cursors = self.dbi.processData(sql, binds, conn, transaction=trans, returnCursor=True)
        result=[]
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
        return result
