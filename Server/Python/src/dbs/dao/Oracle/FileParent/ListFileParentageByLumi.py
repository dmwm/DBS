#!/usr/bin/env python
"""
This module provides File Parentage by matching (run lumi) pairs.
child_bloc_name is required so we are dealing only a block of child data. If it is not provided, return [].
child_lfn_list is empty for the current use case. We keep it there just for the future. Searching on child_lfn_list will slow down the query significantly. 

Y Guo July 13, 2018 
"""
from types import GeneratorType
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSDaoTools import create_token_generator

class ListFileParentageByLumi(DBFormatter):
    """
    FileParent List by (Run,Lumi) DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        
        self.parent_sql = """
        select run_num as R, Lumi_section_num as L, file_id as pid from {owner}file_lumis fl
        where fl.file_id in (select file_id from {owner}files f
        where F.DATASET_ID in (select parent_dataset_id from {owner}dataset_parents dp
        inner join {owner}datasets d on d.dataset_id=DP.THIS_DATASET_ID
        """.format(owner=self.owner)

        self.child_sql = """
        select  run_num as R, Lumi_section_num as L, file_id as cid from {owner}file_lumis fl
        where fl.file_id in (select file_id from {owner}files f
        inner join {owner}blocks b on f.block_id = b.block_id
        """.format(owner=self.owner)

    def execute(self, conn, child_block_name='', child_lfn_list=[], transaction=False):
        sql = ''
        binds = {}
        child_ds_name = ''
        child_where = ''
        if child_block_name:
            child_ds_name = child_block_name.split('#')[0]
            parent_where = " where d.dataset = :child_ds_name ))"
            binds ={"child_ds_name": child_ds_name}
        else:
            dbsExceptionHandler('dbsException-invalid-input', "Missing child block_name for listFileParentsByLumi. ")
        #
        if not child_lfn_list:
            # most use cases 
            child_where = " where b.block_name = :child_block_name )"
            binds.update({"child_block_name": child_block_name})
            sql = """
            with
            parents as
            (            
            """  +\
            self.parent_sql +\
            parent_where +\
            """), 
 
            """+\
            """
            children as
            (
            """ +\
            self.child_sql +\
            child_where  +\
            """)
            select distinct cid, pid from children c
                inner join parents p on c.R = p.R and c.L = p.L 
            """  
        else:
            # not commom 
            child_where = """ where b.block_name = :child_block_name 
                              and f.logical_file_name in (SELECT TOKEN FROM TOKEN_GENERATOR) ))
                          """
            lfn_generator, bind = create_token_generator(child_lfn_list)
            binds.update(bind)
            sql = lfn_generator +\
            """
            with
            parents as
            (            
            """  +\
            self.parent_sql +\
            parent_where +\
            """), 
 
            """+\
            """
            children as
            (
            """ +\
            self.child_sql +\
            child_where  +\
            """)
            select distinct cid, pid from children c
                inner join parents p on c.R = p.R and c.L = p.L 
            """
        print(sql)


        r = self.dbi.processData(sql, binds, conn, transaction=transaction)
        print(self.format(r))
        return self.format(r)
        """
        cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
        for i in cursors:
            d = self.formatCursor(i)
            if isinstance(d, list) or isinstance(d, GeneratorType):
                for elem in d:
                    yield elem
            elif d: 
                yield d
        """
