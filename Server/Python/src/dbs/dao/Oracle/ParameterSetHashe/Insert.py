#!/usr/bin/env python
""" DAO Object for ParameterSetHashes table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:21 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sPARAMETER_SET_HASHES ( PARAMETER_SET_HASH_ID, HASH, NAME) VALUES (:parametersethashid, :hash, :name) % (self.owner) ;"""

    def getBinds_delme( self, parameter_set_hashesObj ):
            binds = {}
            if type(parameter_set_hashesObj) == type ('object'):
            	binds = {
			'parametersethashid' : parameter_set_hashesObj['parametersethashid'],
			'hash' : parameter_set_hashesObj['hash'],
			'name' : parameter_set_hashesObj['name'],
                 }

            elif type(parameter_set_hashesObj) == type([]):
               binds = []
               for item in parameter_set_hashesObj:
                   binds.append({
 	                'parametersethashid' : item['parametersethashid'],
 	                'hash' : item['hash'],
 	                'name' : item['name'],
 	                })
               return binds


    def execute( self, parameter_set_hashesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( parameter_set_hashesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


