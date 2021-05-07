#!/usr/bin/env python
""" DAO Object for FileParents table. The input is child and parent file id. """ 
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
                    """insert into {owner}file_parents 
                       (this_file_id, parent_file_id) 
                       values(:this_file_id, :parent_file_id)
                    """.format(owner=self.owner)


        self.sql_sel = \
                        """select distinct file_id from {owner}files f
                           inner join {owner}blocks b on f.block_id=b.block_id
                           where b.block_name = :block_name 
                        """.format(owner=self.owner)


    def execute( self, conn, daoinput, transaction = False ):
        """
        daoinput must be validated to have the following keys:
        child_parent_id__list[[cid, pid],...], block_name
        """
        binds = {} 
        bindlist=[]
        
        if isinstance(daoinput, dict) and "block_name" in list(daoinput.keys()):
            binds = {"block_name": daoinput["block_name"]}
            r = self.dbi.processData(self.sql_sel, binds, conn, False)
            bfile = self.format(r)
            bfile_list = []
            for f in bfile:
                bfile_list.append(f[0])           
            if "child_parent_id_list" in list(daoinput.keys()):
                files = []
                for i in daoinput["child_parent_id_list"]:
                    files.append(i[0])
                if set(files)-set(bfile_list):
                    dbsExceptionHandler('dbsException-invalid-input2', "Files required in the same block for FileParent/insert2 dao.", self.logger.exception) 
            else:
                dbsExceptionHandler('dbsException-invalid-input2', "child_parent_id_list required for FileParent/insert2 dao.", self.logger.exception) 
        else:
            dbsExceptionHandler('dbsException-invalid-input2', "Block_name required in the same block for FileParent/insert2 dao.", self.logger.exception)
        binds = {} 
        for pf in daoinput["child_parent_id_list"]:
            binds = {"this_file_id":pf[0], "parent_file_id": pf[1]}
            bindlist.append(binds) 
        self.dbi.processData(self.sql, bindlist, conn, transaction)
