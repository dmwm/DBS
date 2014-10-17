#!/usr/bin/env python
"""
File.SummaryList provides summary data(number of files, even counts and number of lumi sections) 
for a given Block or Dataset. Full Block and dataset names are expected. Block name will exceeds 
dataset name. 
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.utils.DBSTransformInputType import run_tuple
from dbs.utils.DBSDaoTools import create_token_generator

class SummaryList(DBFormatter):
    """
    File SummaryList DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

    def execute(self, conn, block_name="", dataset="",  run_num=-1, validFileOnly=0, transaction=False):
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed","Oracle/File/SummaryList. Expects db connection from upper layer.")

        binds = {}
        whererun = ''
        run_list = []
        wheresql_run_list = ''
        wheresql_run_range = ''
        wheresql_isFileValid = ''
        sql = ''
        #
        if validFileOnly != 0:
            wheresql_isFileValid =' and f.is_file_valid = 1 '
        #
        if run_num != -1:
            #
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, (long,int)):
                    #if not wheresql_run_list:
                    #    wheresql_run_list = " fl.RUN_NUM = :run_list "
                    run_list.append(str(r))
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.")
                    wheresql_run_range = " fl.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":r[0]})
                    binds.update({"maxrun":r[1]})
            #
            if run_list:
                wheresql_run_list = " fl.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) "
                run_generator, run_binds = create_token_generator(run_list)
                sql =  "{run_generator}".format(run_generator=run_generator)
                binds.update(run_binds)
            if wheresql_run_list and wheresql_run_range:
                whererun = wheresql_run_range + " or " + wheresql_run_list
            elif wheresql_run_list:
                whererun = wheresql_run_list
            elif wheresql_run_range:
                whererun = wheresql_run_range
        if block_name:
            if run_num != -1:
                #
                sql = sql +\
                   """
                    select
                   (select count(f.file_id)  from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                     and f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun} )
                    ) as num_file,
                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ),0) as num_event,
                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,
                   (select count(distinct b.block_id) from {owner}blocks b
                   join {owner}files f on f.block_id=b.block_id
                   where b.block_name=:block_name {wheresql_isFileValid} and
                   f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                   )as num_block,
                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}blocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and {whererun} )
                   ) as num_lumi
                   from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid)
                binds.update({"block_name":block_name})
            else:
                sql = """
                    select
                   (select count(f.file_id)  from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ) as file_size,

                    (select count(block_id) from {owner}blocks where block_name=:block_name
                    ) as num_block,

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from {owner}files f
                    join {owner}file_lumis l on l.file_id=f.file_id
                    join {owner}blocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name {wheresql_isFileValid})
                   ) as num_lumi
                   from dual
                    """ .format(owner=self.owner, wheresql_isFileValid=wheresql_isFileValid)
                binds.update({"block_name":block_name})

        elif dataset:
            if run_num != -1:
                sql = sql + \
                    """
                    select
                    (select count(f.file_id)  from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,

                    (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}datasets d on d.dataset_id = b.dataset_id
                     join {owner}files f on f.block_id = b.block_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_block,

                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}datasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset {wheresql_isFileValid} and {whererun} )
                   ) as num_lumi
                    from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid)
                binds.update({"dataset":dataset})
            else:
                sql = """ \
                    select
                   (select count(f.file_id)  from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ) as file_size,

                    (select count(b.block_id) from {owner}blocks b
                     join {owner}datasets d on d.dataset_id = b.dataset_id
                     where d.dataset=:dataset
                    ) as num_block,

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from {owner}files f
                    join {owner}file_lumis l on l.file_id=f.file_id
                    join {owner}datasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset {wheresql_isFileValid})
                   ) as num_lumi
                    from dual
                    """.format(owner=self.owner, wheresql_isFileValid=wheresql_isFileValid)
                binds.update({"dataset":dataset})
        else:
            return []

	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result=[]
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
	return result
