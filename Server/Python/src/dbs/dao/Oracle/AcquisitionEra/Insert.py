# DAO Object for AcquisitionEra table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:23 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO ACQUISITION_ERAS(ACQUISITION_ERA_ID, ACQUISITION_ERA_NAME, CREATION_DATE, CREATE_BY, DESCRIPTION) VALUES (:acquisitioneraid, :acquisitioneraname, :creationdate, :createby, :description);"""

    def getBinds( self, acquisition_erasObj ):
            binds = {}
            if type(acquisition_erasObj) == type ('object'):
            	binds = {
			'acquisitioneraid' : acquisition_erasObj['acquisitioneraid'],
			'acquisitioneraname' : acquisition_erasObj['acquisitioneraname'],
			'creationdate' : acquisition_erasObj['creationdate'],
			'createby' : acquisition_erasObj['createby'],
			'description' : acquisition_erasObj['description'],
                 }

            elif type(acquisition_erasObj) == type([]):
               binds = []
               for item in acquisition_erasObj:
                   binds.append({
 	                'acquisitioneraid' : item['acquisitioneraid'],
 	                'acquisitioneraname' : item['acquisitioneraname'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'description' : item['description'],
 	                })
               return binds


    def execute( self, acquisition_erasObj ):
            binds = self.getBinds(acquisition_erasObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return