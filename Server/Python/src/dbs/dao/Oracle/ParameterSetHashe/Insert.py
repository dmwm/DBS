#!/usr/bin/env python
""" DAO Object for ParameterSetHashes table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/12/21 21:05:41 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sPARAMETER_SET_HASHES ( PARAMETER_SET_HASH_ID, HASH, NAME) VALUES (:parameter_set_hash_id, :hash, :name)""" % (self.owner)

    def execute( self, psetHashObj, conn=None, transaction=False ):
            result = self.dbi.processData(self.sql, psetHashObj, conn, transaction)
            return

