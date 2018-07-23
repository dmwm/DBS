#!/usr/bin/env python
""" DAO Object for BlockParents table.
    Inputs are child block_name and a list of parent logical__file_name.  
    Y. Guo
    July 18, 2018
""" 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError

class Insert3(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql =\
                  """insert into {owner}block_parents (this_block_id, parent_block_id) 
                          values ((select block_id as this_block_id from {owner}blocks where block_name=:block_name), 
                                  :parent_block_id )
                  """.format(owner=self.owner)

        self.sql2 =\
                  """select distinct block_id as parent_block_id from {owner}files where file_id=:parent_file_id
                  """.format(owner=self.owner)

    def execute( self, conn, daoinput, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/BlockParent/Insert. Expects db connection from upper layer.", self.logger.exception)
            
        binds = {}
        bindlist = [] 
        for f in daoinput["child_parent_id_list"]:
            binds = {"parent_file_id":f[1]}
            bindlist.append(binds)
        r =  self.dbi.processData(self.sql2, bindlist, conn, False)
        p_bk_id = self.format(r)
        c_block_name = daoinput["block_name"]
        for b in p_bk_id:
            try:
                binds = {"block_name":c_block_name, "parent_block_id":b[0]}
	        self.dbi.processData(self.sql, binds, conn, transaction)
            except SQLAlchemyIntegrityError as ex:
                if (str(ex).find("ORA-00001") != -1 and (str(ex).find("PK_BP") != -1 or str(ex).lower().find("duplicate") != -1)):
                    pass
                elif str(ex).find("ORA-01400") != -1:
                    raise
                else:
                    raise
