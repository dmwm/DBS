#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/11/16 20:29:06 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sFILE_PARENTS ( FILE_PARENT_ID, THIS_FILE_ID, PARENT_FILE_ID) 
	     VALUES (:FILE_PARENT_ID, :THIS_FILE_ID, :FILE_PARENT_LFN) % (self.owner) ;"""

    def getBinds_delme( self, file_parentsObj ):
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


    def execute( self, file_parentsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( file_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


