#!/usr/bin/env python
""" DAO Object for ProcessingEras table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sPROCESSING_ERAS ( PROCESSING_ERA_ID, PROCESSING_VERSION, CREATION_DATE, CREATE_BY, DESCRIPTION) 
			    VALUES (:processing_era_id, :processing_version, :creation_date, :create_by, :description)""" % (self.owner) 

    def execute( self, conn, binds, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ProcessingEra/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return
