#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/10/20 02:19:22 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sPRIMARY_DATASETS ( PRIMARY_DS_ID, PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) VALUES (:primarydsid, :primarydsname, :primarydstypeid, :creationdate, :createby) % (self.owner) ;"""

    def getBinds_delme( self, primary_datasetsObj ):
            binds = {}
            if type(primary_datasetsObj) == type ('object'):
            	binds = {
			'primarydsid' : primary_datasetsObj['primarydsid'],
			'primarydsname' : primary_datasetsObj['primarydsname'],
			'primarydstypeid' : primary_datasetsObj['primarydstypeid'],
			'creationdate' : primary_datasetsObj['creationdate'],
			'createby' : primary_datasetsObj['createby'],
                 }

            elif type(primary_datasetsObj) == type([]):
               binds = []
               for item in primary_datasetsObj:
                   binds.append({
 	                'primarydsid' : item['primarydsid'],
 	                'primarydsname' : item['primarydsname'],
 	                'primarydstypeid' : item['primarydstypeid'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                })
               return binds


    def execute( self, primary_datasetsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( primary_datasetsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


