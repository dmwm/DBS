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

    def execute(self, conn, block_name="", dataset="",  run=-1, transaction=False):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/File/SummaryList. Expects db connection from upper layer.")

        binds = {}
        whererun = ''
        run_list = []
        wheresql_run_list = ''
        wheresql_run_range = ''
        if run != -1:
            #
            for r in parseRunRange(run):
                if isinstance(r, str) or isinstance(r, int):
                    if not wheresql_run_list:
                        wheresql_run_list = " fl.RUN_NUM = :run_list "
                    run_list.append(r)
                if isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.")
                    wheresql_run_range = " fl.RUN_NUM between :minrun and :maxrun "
                    binds.update({"minrun":r[0]})
                    binds.update({"maxrun":r[1]})
            if wheresql_run_list and wheresql_run_range:
                whererun = wheresql_run_range + " and " + wheresql_run_list
            elif wheresql_run_list:
                whererun = wheresql_run_list
            elif wheresql_run_range:
                whererun = wheresql_run_range
        if block_name:
            if run != -1:
                #
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                     and f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s )    
                    ) as num_file,
                    nvl((select sum(f.event_count) event_count from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)   
                    ),0) as num_event,
                    (select nvl(sum(f.file_size),0) file_size from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name and 
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)
                    ) as file_size,
                   (select count(distinct b.block_id) from %sblocks b 
                   join %sfiles f on f.block_id=b.block_id
                   where b.block_name=:block_name and  
                   f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)
                   )as num_block, 
                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from %sfiles f
                    join %sfile_lumis fl on fl.file_id=f.file_id 
                    join %sblocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name and %s )
                   ) as num_lumi
                   from dual
                    """ %((self.owner,)*3, whererun, (self.owner,)*3, whererun, (self.owner,)*3, whererun, (self.owner,)*3,\
                            whererun,(self.owner,)*3, whererun)
                binds.update({"block_name":block_name})
            else:
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as num_file,
                    
                    nvl((select sum(f.event_count) event_count from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ),0) as num_event,

                    (select nvl(sum(f.file_size),0) file_size from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as file_size,
                    
                    (select count(block_id) from %sblocks where block_name=:block_name
                    ) as num_block, 
                    
                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sblocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name)
                   ) as num_lumi
                   from dual
                    """ %((self.owner,)*10)
                binds.update({"block_name":block_name})

        elif dataset:
            if run != -1:
                sql = """
                    select 
                    (select count(f.file_id)  from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)   
                    ) as num_file,
                       
                    nvl((select sum(f.event_count) event_count from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)
                    ),0) as num_event,
     
                    (select nvl(sum(f.file_size),0) file_size from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)
                    ) as file_size,

                    (select count(distinct b.block_id) from %sblocks b 
                     join %sdatasets d on d.dataset_id = b.dataset_id 
                     join %sfiles f on f.block_id = b.block_id
                     where d.dataset=:dataset and 
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where %s)
                    ) as num_block, 

                   (select count(*) from (select distinct fl.lumi_section_num, fl.run_num from %sfiles f
                    join %sfile_lumis fl on fl.file_id=f.file_id 
                    join %sdatasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset and %s )
                   ) as num_lumi 
                    from dual
                    """ %(self.owner,self.owner, self.owner, whererun,self.owner,self.owner,self.owner, \
                        whererun,self.owner,self.owner,self.owner,whererun,self.owner,self.owner,self.owner,self.owner, \
                        whererun,self.owner,self.owner,self.owner,whererun   )
                binds.update({"dataset":dataset})
            else:
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as num_file,
                       
                    nvl((select sum(f.event_count) event_count from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ),0) as num_event,
     
                    (select nvl(sum(f.file_size),0) file_size from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as file_size,

                    (select count(b.block_id) from %sblocks b 
                     join %sdatasets d on d.dataset_id = b.dataset_id 
                     where d.dataset=:dataset
                    ) as num_block, 

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sdatasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset)
                   ) as num_lumi 
                    from dual
                    """ %((self.owner,)*11)
                binds.update({"dataset":dataset})
        else:
            return []

        if run_list:
            newbinds = []
            for r in run_list:
                b = {}
                b.update(binds)
                b["run_list"] = r
                newbinds.append(b)
            binds = newbinds

	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result=[]
        for i in range(len(cursors)):
            result.extend(self.formatCursor(cursors[i]))
	return result
