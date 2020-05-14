#!/usr/bin/env python
"""
This module provides IDs of File, lumi and run of the parent dataset by a given block name.
Y Guo May 1, 2020
"""
from __future__ import print_function
from types import GeneratorType
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSDaoTools import create_token_generator

class ListParentDatasetFileLumiIds(DBFormatter):
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        
        self.parent_sql = """
        select run_num as R, Lumi_section_num as L, file_id as pfid from {owner}file_lumis fl
        where fl.file_id in (select file_id from {owner}files f
        where F.DATASET_ID in (select parent_dataset_id from {owner}dataset_parents dp
        inner join {owner}datasets d on d.dataset_id=DP.THIS_DATASET_ID
        where d.dataset = :dataset )) order by pfid
        """.format(owner=self.owner)

    def execute(self, conn, dataset='', transaction=False):
        #import time
        sql = ''
        binds = {}
        if dataset:
            binds ={"dataset":dataset}
        else:
            dbsExceptionHandler('dbsException-invalid-input', "Missing child dataste for listParentDatasetFileLumiIds. ")
        sql = self.parent_sql
        print(sql)

        #r = self.dbi.processData(sql, binds, conn, transaction=transaction)
        #return self.format(r)
        #This cursors is a list with len=1 ! YG May13 2020
        #t2 = time.time() 
        cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
        #print("sql time is: %.4f" %(time.time()-t2))
        #t1 = time.time()
        for i in cursors:
            data = self.formatCursor(i, size=1000)
            d = {}
            run_lumi = []
            fid = None
            n = 0
            for i in data:
                n += 1
                if n < 5:
                    print (i)
                r = i['r']
                l = i['l']
                f = i['pfid']
                if fid is None:
                    fid = f
                    run_lumi.append((r,l))
                elif f != fid and fid is not None:
                    d[fid] = run_lumi
                    yield d
                    del d[fid]
                    run_lumi = []
                    fid = f
                    run_lumi.append((r,l))
                else:
                    run_lumi.append((r, l))
            d[fid] = run_lumi
            yield d
            del run_lumi
            del d
            #print("data orignization time is: %.4f" %(time.time()-t1))
