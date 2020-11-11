#!/usr/bin/env python
"""
File.SummaryList provides summary data(number of files, even counts and number of lumi sections) 
for a given Block or Dataset. Full Block and dataset names are expected. Block name will exceeds 
dataset name. 
"""

from types import GeneratorType
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
        self.logger = logger

    def execute(self, conn, block_name="", dataset="",  run_num=-1, validFileOnly=0, sumOverLumi=0, transaction=False):
        binds = {}
        whererun = ''
        run_list = []
        wheresql_run_list = ''
        wheresql_run_range = ''
        wheresql_isFileValid = ''
	join_valid_ds1 = ''
	join_valid_ds2 = ''
        join_bk_fl = ''
        sql = ''
        #
	if int(validFileOnly) == 1 :
            wheresql_isFileValid =""" and f.is_file_valid = 1 and DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') """
            join_valid_ds1 = """ JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID
                                JOIN %sDATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID      
                             """% ((self.owner,)*2)  
            join_valid_ds2 = """ JOIN %sDATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID      
                             """% ((self.owner,)*1)
            join_bk_fl     = """ join %sfiles f on f.block_id = b.block_id 
                             """ % ((self.owner,)*1)
        #
        if run_num != -1:
            #
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, (long, int)):
                    #if not wheresql_run_list:
                    #    wheresql_run_list = " fl.RUN_NUM = :run_list "
                    run_list.append(str(r))
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.",
				self.logger.exception)
                    wheresql_run_range = " fl.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":int(r[0])})
                    binds.update({"maxrun":int(r[1])})
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
        self.logger.debug('sumOverLumi=%s' %sumOverLumi)
        if block_name:
            if run_num != -1:
                if int(sumOverLumi) == 0:
                #
                    sql = sql +\
                    """
                    select
                    (select count(f.file_id)  from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                     and f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun} )
                    ) as num_file,
                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ),0) as num_event,
                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,
                     (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}files f on f.block_id=b.block_id {join_valid_ds1}
                     where b.block_name=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                     )as num_block,
                    (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                    where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and {whererun} )
                    ) as num_lumi
                    from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds1=join_valid_ds1)
                    binds.update({"block_name":block_name})
                elif int(sumOverLumi) == 1:
                    sql = sql +\
                    """
                    select
                    (select count(f.file_id)  from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                     and f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun} )
                    ) as num_file,
                    (
                     with myFiles as
                     (select file_id from {owner}files f 
                      join {owner}blocks b on b.block_id = f.block_id {join_valid_ds1}
                      where b.block_name=:block_name  {wheresql_isFileValid}
                     ) 
                    select sum(fl.event_count) event_count from {owner}file_lumis fl
                    join myFiles on myFiles.file_id = fl.file_id
                    where {whererun} and  
                    not exists (select fl2.file_id from {owner}file_lumis fl2
                               join myFiles on myFiles.file_id = fl2.file_id 
                               where fl2.event_count is null )
                    ) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,
                     (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}files f on f.block_id=b.block_id {join_valid_ds1}
                     where b.block_name=:block_name {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                     )as num_block,
                    (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                    where b.BLOCK_NAME=:block_name {wheresql_isFileValid} and {whererun} )
                    ) as num_lumi
                    from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds1=join_valid_ds1)
                    binds.update({"block_name":block_name})

            else:
                sql = """
                    select
                   (select count(f.file_id)  from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                     where b.BLOCK_NAME=:block_name {wheresql_isFileValid}
                    ) as file_size,

                    (select nvl(count(distinct b.block_id),0) from {owner}blocks b
                     {join_bk_fl}
                     {join_valid_ds1}
                     where b.block_name=:block_name {wheresql_isFileValid}
                    ) as num_block,

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from {owner}files f
                    join {owner}file_lumis l on l.file_id=f.file_id
                    join {owner}blocks b on b.BLOCK_ID = f.block_id {join_valid_ds1}
                    where b.BLOCK_NAME=:block_name {wheresql_isFileValid})
                   ) as num_lumi
                   from dual
                    """ .format(owner=self.owner, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds1=join_valid_ds1, join_bk_fl=join_bk_fl)
                binds.update({"block_name":block_name}) 

        elif dataset:
            if run_num != -1:
                if int(sumOverLumi) == 0:
                    sql = sql + \
                    """
                    select
                    (select count(f.file_id)  from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id  {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,

                    (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}datasets d on d.dataset_id = b.dataset_id {join_valid_ds2}
                     join {owner}files f on f.block_id = b.block_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_block,

                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                    where d.dataset=:dataset {wheresql_isFileValid} and {whererun} )
                   ) as num_lumi
                    from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds2=join_valid_ds2)
                    binds.update({"dataset":dataset})
                elif int(sumOverLumi) == 1:
                    sql = sql + \
                    """
                    select
                    (select count(f.file_id)  from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_file,
                    (
                    with myFiles as
                     (select file_id from {owner}files f 
                      join {owner}datasets d on d.dataset_id = f.dataset_id {join_valid_ds2}
                      where d.dataset=:dataset  {wheresql_isFileValid}
                     ) 
                    select sum(fl.event_count) event_count from {owner}file_lumis fl
                    join myFiles on myFiles.file_id = fl.file_id
                    where {whererun} and  
                    not exists (select fl2.file_id from {owner}file_lumis fl2
                               join myFiles on myFiles.file_id = fl2.file_id 
                               where fl2.event_count is null )
                    ) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as file_size,

                    (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}datasets d on d.dataset_id = b.dataset_id {join_valid_ds2}
                     join {owner}files f on f.block_id = b.block_id
                     where d.dataset=:dataset {wheresql_isFileValid} and
                     f.FILE_ID in (select fl.file_id from {owner}file_lumis fl where {whererun})
                    ) as num_block,

                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {owner}files f
                    join {owner}file_lumis fl on fl.file_id=f.file_id
                    join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                    where d.dataset=:dataset {wheresql_isFileValid} and {whererun} )
                   ) as num_lumi
                    from dual
                    """.format(owner=self.owner, whererun=whererun, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds2=join_valid_ds2)
                binds.update({"dataset":dataset})
            else:
                sql = """ \
                    select
                   (select count(f.file_id)  from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ) as num_file,

                    nvl((select sum(f.event_count) event_count from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from {owner}files f
                     join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ) as file_size,

                    (select count(distinct b.block_id) from {owner}blocks b
                     join {owner}datasets d on d.dataset_id = b.dataset_id {join_valid_ds2}
                     {join_bk_fl}
                     where d.dataset=:dataset {wheresql_isFileValid}
                    ) as num_block,

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from {owner}file_lumis l
                    join {owner}files f on f.file_id=l.file_id
                    join {owner}datasets d on d.DATASET_ID = f.dataset_id {join_valid_ds2}
                    where d.dataset=:dataset {wheresql_isFileValid})
                   ) as num_lumi
                    from dual
                    """.format(owner=self.owner, wheresql_isFileValid=wheresql_isFileValid, join_valid_ds2=join_valid_ds2, join_bk_fl=join_bk_fl)
                binds.update({"dataset":dataset})
        else:
            return 
        #self.logger.error(sql)
        #self.logger.error(binds)
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        for i in cursors:
            d = self.formatCursor(i, size=100)
            if isinstance(d, list) or isinstance(d, GeneratorType):
                for elem in d:
                    yield elem
            elif d:
                yield d
