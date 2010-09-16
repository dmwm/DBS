#!/usr/bin/env python
""" DAO Object for Blocks table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2009/12/23 20:38:11 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """Block Insert DAO Class"""

    def execute( self, daoinput, conn = None, transaction = False):
        """daoinput must be validated to have the following keys:
        block_id, block_name, dataset_id
	It May have the following keys:
	open_for_writing, origin_site(id), block_size,
        file_count, creation_date, create_by, lastmodification_date, lastmodified_by
        """
        try:
            self.executeSingle(daoinput, "BLOCKS", conn, transaction)
        except Exception:
            raise
            

