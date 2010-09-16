#!/usr/bin/env python
""" DAO Object for ParameterSetHashes table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2010/02/11 22:52:00 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sPARAMETER_SET_HASHES ( PARAMETER_SET_HASH_ID, PSET_HASH, NAME) VALUES (:parameter_set_hash_id, :pset_hash, :name)""" % (self.owner)

    def execute( self, psetHashObj, conn=None, transaction=False ):
	try:
	    result = self.dbi.processData(self.sql, psetHashObj, conn, transaction)
	except Exception, ex:
	    if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		pass
	    else:
		raise

