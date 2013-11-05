#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

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
        self.sql = """ SELECT  B.BLOCK_NAME """ 
        self.fromsql = """  FROM %sBLOCKS B """ % self.owner

    def execute(self, conn, dataset="", block_name="", data_tier_name="", origin_site_name="", logical_file_name="",
                run_num=-1, min_cdate=0, max_cdate=0, min_ldate=0, max_ldate=0, cdate=0,
                ldate=0, transaction = False):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        """
        binds = {}

        basesql = self.sql
        joinsql = ""
        wheresql = ""
        generatedsql = ""

        if logical_file_name and logical_file_name != "%":
            joinsql +=  " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID " %(self.owner)
            op =  ("=", "like")["%" in logical_file_name]
            wheresql +=  " WHERE LOGICAL_FILE_NAME %s :logical_file_name " % op
            binds.update( logical_file_name = logical_file_name )

        if  block_name and  block_name !="%":
            andorwhere = ("WHERE", "AND")[bool(wheresql)]
            op =  ("=", "like")["%" in block_name]
            wheresql +=  " %s B.BLOCK_NAME %s :block_name " % ((andorwhere, op))
            binds.update( block_name = block_name )

        if data_tier_name or (dataset and dataset!="%"):
            joinsql += "JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID "  % (self.owner)
            andorwhere = ("WHERE", "AND")[bool(wheresql)]
            if dataset:
                op = ("=", "like")["%" in dataset]
                wheresql += " %s DS.DATASET %s :dataset " % ((andorwhere, op))
                binds.update(dataset=dataset)
            if data_tier_name:
                joinsql += "JOIN {owner}DATA_TIERS DT ON DS.DATA_TIER_ID=DT.DATA_TIER_ID ".format(owner=self.owner)
                wheresql += " %s DT.DATA_TIER_NAME=:data_tier_name " % (andorwhere)
                binds.update(data_tier_name=data_tier_name)

        if origin_site_name and  origin_site_name != "%":
            op = ("=", "like")["%" in origin_site_name]
            wheresql += " AND B.ORIGIN_SITE_NAME %s :origin_site_name " % op
            binds.update(origin_site_name = origin_site_name)

        if cdate != 0:
            wheresql += "AND B.CREATION_DATE = :cdate "
            binds.update(cdate = cdate)
        elif min_cdate != 0 and max_cdate != 0:
            wheresql += "AND B.CREATION_DATE BETWEEN :min_cdate and :max_cdate "
            binds.update(min_cdate = min_cdate)
            binds.update(max_cdate = max_cdate)
        elif min_cdate != 0 and max_cdate == 0:
            wheresql += "AND B.CREATION_DATE > :min_cdate "
            binds.update(min_cdate = min_cdate)
        elif min_cdate ==0 and max_cdate != 0:
            wheresql += "AND B.CREATION_DATE < :max_cdate "
            binds.update(max_cdate = max_cdate)
        else:
            pass
        if ldate != 0:
            wheresql += "AND B.LAST_MODIFICATION_DATE = :ldate "
            binds.update(ldate = ldate)
        elif min_ldate != 0 and max_ldate != 0:
            wheresql += "AND B.LAST_MODIFICATION_DATE BETWEEN :min_ldate and :max_ldate "
            binds.update(min_ldate = min_ldate)
            binds.update(max_ldate = max_ldate)
        elif min_ldate != 0 and max_ldate == 0:
            wheresql += "AND B.LAST_MODIFICATION_DATE > :min_ldate "
            binds.update(min_ldate = min_ldate)
        elif min_cdate ==0 and max_cdate != 0:
            wheresql += "AND B.LAST_MODIFICATION_DATE < :max_ldate "
            binds.update(max_ldate = max_ldate)
        else:
            pass

        #one may provide a list of runs , so it has to be the last one in building the bind.
        if run_num !=-1 :
            basesql = basesql.replace("SELECT", "SELECT DISTINCT") + " , FLM.RUN_NUM  "
            if not logical_file_name:
                joinsql +=  " JOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID " %(self.owner)
            joinsql += " JOIN %sFILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID " %(self.owner)
            run_list=[]
            wheresql_run_list=''
            wheresql_run_range=''
            #
            for r in parseRunRange(run_num):
                if isinstance(r, str) or isinstance(r, int) or isinstance(r, long):
                    run_list.append(str(r))
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run_num range must be apart at least by 1.")
                    wheresql_run_range = " FLM.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":r[0]})
                    binds.update({"maxrun":r[1]})
            #
            if run_list:
                wheresql_run_list = " FLM.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
                generatedsql, run_binds = create_token_generator(run_list)
                binds.update(run_binds)
            #  
            if wheresql_run_range and wheresql_run_list:
                wheresql += " and (" + wheresql_run_range + " or " +  wheresql_run_list + " )"
            elif wheresql_run_range and wheresql_run_list:
                wheresql +=  " and " + wheresql_run_range
            elif not wheresql_run_range and wheresql_run_list:
                wheresql += " and "  + wheresql_run_list
        #
        sql = " ".join((generatedsql ,basesql, self.fromsql, joinsql, wheresql))

        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result=[]
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
        return result
