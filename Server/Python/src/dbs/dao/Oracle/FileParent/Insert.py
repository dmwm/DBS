# DAO Object for FileParent table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:28 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO FILE_PARENTS(FILE_PARENT_ID, THIS_FILE_ID, PARENT_FILE_ID) VALUES (:fileparentid, :thisfileid, :parentfileid);"""

    def getBinds( self, file_parentsObj ):
            binds = {}
            if type(file_parentsObj) == type ('object'):
            	binds = {
			'fileparentid' : file_parentsObj['fileparentid'],
			'thisfileid' : file_parentsObj['thisfileid'],
			'parentfileid' : file_parentsObj['parentfileid'],
                 }

            elif type(file_parentsObj) == type([]):
               binds = []
               for item in file_parentsObj:
                   binds.append({
 	                'fileparentid' : item['fileparentid'],
 	                'thisfileid' : item['thisfileid'],
 	                'parentfileid' : item['parentfileid'],
 	                })
               return binds


    def execute( self, file_parentsObj ):
            binds = self.getBinds(file_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return