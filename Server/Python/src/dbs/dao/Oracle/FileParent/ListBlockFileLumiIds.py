#!/usr/bin/env python
"""
This module provides the IDs of File, lumi and run for a give block.

Y Guo May 1, 2020
"""
from __future__ import print_function
from types import GeneratorType
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSDaoTools import create_token_generator

class ListBlockFileLumiIds(DBFormatter):
    """
    FileParent List by (Run,Lumi) DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        
        self.child_sql = """
        select  run_num as R, Lumi_section_num as L, file_id as cfid from {owner}file_lumis fl
        where fl.file_id in (select file_id from {owner}files f
        inner join {owner}blocks b on f.block_id = b.block_id 
        """.format(owner=self.owner)

    def execute(self, conn, block_name='', child_lfn_list=[], transaction=False):
        sql = ''
        binds = {}
        child_ds_name = ''
        child_where = ''
        if not child_lfn_list:
            # most use cases 
            child_where = " where b.block_name = :block_name )"
            binds.update({"block_name": block_name})
            sql = self.child_sql + child_where + " order by cfid " 
        else:
            # not commom 
            child_where = """ where b.block_name = :child_block_name 
                              and f.logical_file_name in (SELECT TOKEN FROM TOKEN_GENERATOR) ))
                          """
            lfn_generator, bind = create_token_generator(child_lfn_list)
            binds.update(bind)
            sql = lfn_generator +\
            self.child_sql +\
            child_where + " order by cfid "
        print(sql)


        #r = self.dbi.processData(sql, binds, conn, transaction=transaction)
        #print(self.format(r))
        #return self.format(r)
        cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
        for i in cursors:
            data = self.formatCursor(i, size=1000)
            d = {}
            run_lumi = []
            fid = None
            for i in data:
                r = i['r']
                l = i['l']
                f = i['cfid']
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

