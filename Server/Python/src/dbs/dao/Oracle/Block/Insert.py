#!/usr/bin/env python
""" DAO Object for Blocks table """ 

__revision__ = "$Revision: 1.9 $"
__version__  = "$Id: Insert.py,v 1.9 2010/03/05 15:32:53 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """Block Insert DAO Class"""

    def execute( self, conn, daoinput,  transaction = False):
        """daoinput must be validated to have the following keys:
        block_id, block_name, dataset_id
	It May have the following keys:
	open_for_writing, origin_site(id), block_size,
        file_count, creation_date, create_by, lastmodification_date, lastmodified_by
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/Block/Insert expects db connection from up layer.")
        self.executeSingle(daoinput, "BLOCKS", conn, transaction)
            

