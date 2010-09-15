# DAO Object for ParameterSetHashe table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:29 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PARAMETER_SET_HASHES(PARAMETER_SET_HASH_ID, HASH, NAME) VALUES (:parametersethashid, :hash, :name);"""

    def getBinds( self, parameter_set_hashesObj ):
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


    def execute( self, parameter_set_hashesObj ):
            binds = self.getBinds(parameter_set_hashesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return