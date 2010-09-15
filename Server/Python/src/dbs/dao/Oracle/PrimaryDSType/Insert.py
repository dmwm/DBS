#!/usr/bin/env python
""" DAO Object for PrimaryDSTypes table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/28 23:08:03 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner
	    
            self.sql = """INSERT INTO %sPRIMARY_DS_TYPES ( PRIMARY_DS_TYPE_ID, PRIMARY_DS_TYPE) VALUES (:primarydstypeid, :primarydstype)""" % (self.owner)

    def getBinds_delme( self, primary_ds_typesObj ):
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


    def execute( self, primary_ds_typesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( primary_ds_typesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


