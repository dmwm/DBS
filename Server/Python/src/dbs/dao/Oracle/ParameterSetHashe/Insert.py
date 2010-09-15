#!/usr/bin/env python
""" DAO Object for ParameterSetHashes table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2010/01/07 17:30:43 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sPARAMETER_SET_HASHES ( PARAMETER_SET_HASH_ID, HASH, NAME) VALUES (:parameter_set_hash_id, :pset_hash, :name)""" % (self.owner)

    def execute( self, psetHashObj, conn=None, transaction=False ):
            result = self.dbi.processData(self.sql, psetHashObj, conn, transaction)
            return

