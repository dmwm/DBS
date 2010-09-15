# DAO Object for AssociatedFile table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:23 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO ASSOCIATED_FILES(ASSOCATED_FILE_ID, THIS_FILE_ID, ASSOCATED_FILE) VALUES (:assocatedfileid, :thisfileid, :assocatedfile);"""

    def getBinds( self, associated_filesObj ):
            binds = {}
            if type(associated_filesObj) == type ('object'):
            	binds = {
			'assocatedfileid' : associated_filesObj['assocatedfileid'],
			'thisfileid' : associated_filesObj['thisfileid'],
			'assocatedfile' : associated_filesObj['assocatedfile'],
                 }

            elif type(associated_filesObj) == type([]):
               binds = []
               for item in associated_filesObj:
                   binds.append({
 	                'assocatedfileid' : item['assocatedfileid'],
 	                'thisfileid' : item['thisfileid'],
 	                'assocatedfile' : item['assocatedfile'],
 	                })
               return binds


    def execute( self, associated_filesObj ):
            binds = self.getBinds(associated_filesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return