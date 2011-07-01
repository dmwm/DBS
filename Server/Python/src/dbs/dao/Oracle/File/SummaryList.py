#!/usr/bin/env python
"""
File.SummaryList provides summary data(number of files, even counts and number of lumi sections) 
for a given Block or Dataset. Full Block and dataset names are expected. Block name will exceeds 
dataset name. 
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

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

    def execute(self, conn, block_name="", dataset="",  run_num=0, transaction=False):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/File/SummaryList. Expects db connection from upper layer.")

        binds = {}

        if block_name:
            if run_num > 0:
                #
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                     and f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)   
                    ) as num_file,
                    
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)   
                    ) as num_event,

                    (select sum(f.file_size) file_size from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name and 
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)
                    ) as file_size,
                    
                   (select count(distinct b.block_id) from %sblocks b 
                   join %sfiles f on f.block_id=b.block_id
                   where b.block_name=:block_name and  
                   f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)
                   )as num_block, 
                    
                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sblocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name and l.run_num=:run_num)
                   ) as num_lumi
                   from dual
                    """ %((self.owner,)*15)
                binds.update({"block_name":block_name})
                binds.update({"run_num":run_num})
            else:
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as num_file,
                    
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as num_event,

                    (select sum(f.file_size) file_size from %sfiles f 
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
            if run_num >0:
                sql = """
                    select 
                    (select count(f.file_id)  from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)   
                    ) as num_file,
                       
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)
                    ) as num_event,
     
                    (select sum(f.file_size) file_size from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset and
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)
                    ) as file_size,

                    (select count(distinct b.block_id) from %sblocks b 
                     join %sdatasets d on d.dataset_id = b.dataset_id 
                     join %sfiles f on f.block_id = b.block_id
                     where d.dataset=:dataset and 
                     f.FILE_ID in (select fl.file_id from %sfile_lumis fl where run_num=:run_num)
                    ) as num_block, 

                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sdatasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset and l.run_num = :run_num)
                   ) as num_lumi 
                    from dual
                    """ %((self.owner,)*16)
                binds.update({"dataset":dataset})
                binds.update({"run_num":run_num})
            else:
                sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as num_file,
                       
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as num_event,
     
                    (select sum(f.file_size) file_size from %sfiles f 
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

	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)

	result = self.formatCursor(cursors[0])
	return result
