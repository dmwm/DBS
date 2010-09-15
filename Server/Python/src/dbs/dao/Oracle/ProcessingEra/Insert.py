# DAO Object for ProcessingEra table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:31 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PROCESSING_ERAS(PROCESSING_ERA_ID, PROCESSING_VERSION, CREATION_DATE, CREATE_BY, DESCRIPTION) VALUES (:processingeraid, :processingversion, :creationdate, :createby, :description);"""

    def getBinds( self, processing_erasObj ):
            binds = {}
            if type(processing_erasObj) == type ('object'):
            	binds = {
			'processingeraid' : processing_erasObj['processingeraid'],
			'processingversion' : processing_erasObj['processingversion'],
			'creationdate' : processing_erasObj['creationdate'],
			'createby' : processing_erasObj['createby'],
			'description' : processing_erasObj['description'],
                 }

            elif type(processing_erasObj) == type([]):
               binds = []
               for item in processing_erasObj:
                   binds.append({
 	                'processingeraid' : item['processingeraid'],
 	                'processingversion' : item['processingversion'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'description' : item['description'],
 	                })
               return binds


    def execute( self, processing_erasObj ):
            binds = self.getBinds(processing_erasObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return