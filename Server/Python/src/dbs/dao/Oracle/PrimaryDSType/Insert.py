# DAO Object for PrimaryDSType table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:30 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PRIMARY_DS_TYPES(PRIMARY_DS_TYPE_ID, PRIMARY_DS_TYPE) VALUES (:primarydstypeid, :primarydstype);"""

    def getBinds( self, primary_ds_typesObj ):
            binds = {}
            if type(primary_ds_typesObj) == type ('object'):
            	binds = {
			'primarydstypeid' : primary_ds_typesObj['primarydstypeid'],
			'primarydstype' : primary_ds_typesObj['primarydstype'],
                 }

            elif type(primary_ds_typesObj) == type([]):
               binds = []
               for item in primary_ds_typesObj:
                   binds.append({
 	                'primarydstypeid' : item['primarydstypeid'],
 	                'primarydstype' : item['primarydstype'],
 	                })
               return binds


    def execute( self, primary_ds_typesObj ):
            binds = self.getBinds(primary_ds_typesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return