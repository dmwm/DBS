#!/usr/bin/env python
"""
This module provides FileLumi.List data access object.
"""
__revision__ = "$Id: List.py,v 1.7 2010/08/05 16:08:24 yuyi Exp $"
__version__ = "$Revision: 1.7 $"


from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

class List(DBFormatter):
    """
    FileLumi List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
"""
SELECT DISTINCT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM
"""

    def execute(self, conn, logical_file_name='', block_name='', run_num=-1, validFileOnly=0, migration=False):
        """
        Lists lumi section numbers with in a file or a block.
        """
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/FileLumi/List. Expects db connection from upper layer.")            
        if logical_file_name:
            binds = {'logical_file_name': logical_file_name}
            if validFileOnly == 0:
                sql = self.sql + """ FROM {owner}FILE_LUMIS FL JOIN {owner}FILES F ON F.FILE_ID = FL.FILE_ID 
                WHERE F.LOGICAL_FILE_NAME = :logical_file_name""".format(owner=self.owner)
            else:
                sql = self.sql + """ FROM {owner}FILE_LUMIS FL JOIN {owner}FILES F ON F.FILE_ID = FL.FILE_ID
                WHERE F.IS_FILE_VALID = 1 and F.LOGICAL_FILE_NAME = :logical_file_name""".format(owner=self.owner)

        elif block_name:
            binds = {'block_name': block_name}
            if validFileOnly == 0:
                sql = self.sql + """ , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME   
                      FROM {owner}FILE_LUMIS FL JOIN {owner}FILES F ON F.FILE_ID = FL.FILE_ID  
                      JOIN {owner}BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID  
                      WHERE B.BLOCK_NAME = :block_name""".format(owner=self.owner)
            else:
                sql = self.sql + """ , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME
                      FROM {owner}FILE_LUMIS FL JOIN {owner}FILES F ON F.FILE_ID = FL.FILE_ID
                      JOIN {owner}BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
                      F.IS_FILE_VALID = 1 and WHERE B.BLOCK_NAME = :block_name""".format(owner=self.owner)
        else:
                dbsExceptionHandler('dbsException-invalid-input', "FileLumi/List: Either logocal_file_name or block_name must be provided.")
          #
        if run_num != -1:
            run_list = []
            wheresql_run_list=''
            wheresql_run_range=''
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):
                    run_list.append(str(r))
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.")
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
                sql += " and (" + wheresql_run_range + " or " +  wheresql_run_list + " )"
            elif wheresql_run_range and not wheresql_run_list:
                sql += " and " + wheresql_run_range
            elif not wheresql_run_range and wheresql_run_list:
                sql += " and " + wheresql_run_list
        #self.logger.debug(sql) 
	#self.logger.debug(binds)
        cursors = self.dbi.processData(sql, binds, conn, transaction=False, returnCursor=True)
        result=[]
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
        #for migration, we need flat format to load the data into another DB.
        if migration:
            return result
        condensed_res=[]
        if logical_file_name:
            run_lumi={}
            for i in result:
                r = i['run_num']
                if r in run_lumi:
                    run_lumi[r].append(i['lumi_section_num'])
                else:
                    run_lumi[r]=[i['lumi_section_num']]
            for k, v in run_lumi.iteritems():
                condensed_res.append({'logical_file_name':logical_file_name, 'run_num':k, 'lumi_section_num':v})
        else:
            file_run_lumi={}
            for i in result:
                r = i['run_num']
                f = i['logical_file_name']
                if (f, r) in file_run_lumi:
                    file_run_lumi[f,r].append(i['lumi_section_num'])
                else:
                    file_run_lumi[f,r] = [i['lumi_section_num']]
            for k, v in file_run_lumi.iteritems():
                condensed_res.append({'logical_file_name':k[0], 'run_num':k[1], 'lumi_section_num':v})
        return condensed_res
