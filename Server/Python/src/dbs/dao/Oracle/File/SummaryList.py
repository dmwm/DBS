#!/usr/bin/env python
"""
File.SummaryList provides summary data(number of files, even counts and number of lumi sections) 
for a given Block or Dataset. Full Block and dataset names are expected. Block name will exceeds 
dataset name. 
"""

from WMCore.Database.DBFormatter import DBFormatter

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

    def execute(self, conn, block_name="", dataset="",  transaction=False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/List expects db connection from upper layer.")
        binds = {}
        #import pdb
        #pdb.set_trace()

        if block_name:
            sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as num_file,
                    
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sblocks b on b.BLOCK_ID = f.block_id
                     where b.BLOCK_NAME=:block_name
                    ) as event_count,
                    
                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sblocks b on b.BLOCK_ID = f.block_id
                    where b.BLOCK_NAME=:block_name)
                   ) as num_lumi
                   from dual

                  """ %((self.owner,)*7)
            binds.update({"block_name":block_name})

        elif dataset: 
            sql = """
                    select 
                   (select count(f.file_id)  from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as num_file,
                    
                    (select sum(f.event_count) event_count from %sfiles f 
                     join %sdatasets d on d.DATASET_ID = f.dataset_id
                     where d.dataset=:dataset
                    ) as event_count,
                    
                   (select count(*) from (select distinct l.lumi_section_num, l.run_num from %sfiles f
                    join %sfile_lumis l on l.file_id=f.file_id 
                    join %sdatasets d on d.DATASET_ID = f.dataset_id
                    where d.dataset=:dataset)
                   ) as num_lumi
                   from dual

                  """ %((self.owner,)*7)

            binds.update({"dataset":dataset})
        else:
            return []


	#print "sql=%s" %sql
	#print "binds=%s" %binds
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	#if len(cursors) != 1 :
	#    raise Exception("File does not exist.")
	result = self.formatCursor(cursors[0])
	return result
