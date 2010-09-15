# DAO Object for PrimaryDataset table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:30 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PRIMARY_DATASETS(PRIMARY_DS_ID, PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) VALUES (:primarydsid, :primarydsname, :primarydstypeid, :creationdate, :createby);"""

    def getBinds( self, primary_datasetsObj ):
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


    def execute( self, primary_datasetsObj ):
            binds = self.getBinds(primary_datasetsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return