# DAO Object for DataTier table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:25 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO DATA_TIERS(DATA_TIER_ID, DATA_TIER_NAME, CREATION_DATE, CREATE_BY) VALUES (:datatierid, :datatiername, :creationdate, :createby);"""

    def getBinds( self, data_tiersObj ):
            binds = {}
            if type(data_tiersObj) == type ('object'):
            	binds = {
			'datatierid' : data_tiersObj['datatierid'],
			'datatiername' : data_tiersObj['datatiername'],
			'creationdate' : data_tiersObj['creationdate'],
			'createby' : data_tiersObj['createby'],
                 }

            elif type(data_tiersObj) == type([]):
               binds = []
               for item in data_tiersObj:
                   binds.append({
 	                'datatierid' : item['datatierid'],
 	                'datatiername' : item['datatiername'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                })
               return binds


    def execute( self, data_tiersObj ):
            binds = self.getBinds(data_tiersObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return