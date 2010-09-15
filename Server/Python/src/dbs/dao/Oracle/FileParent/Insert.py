#!/usr/bin/env python
""" DAO Object for FileParents table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/11/17 19:44:19 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sFILE_PARENTS ( FILE_PARENT_ID, THIS_FILE_ID, PARENT_FILE_ID) 
	     VALUES (:FILE_PARENT_ID, :THIS_FILE_ID, : PARENT_FILE_ID) """  % (self.owner)

    def getBinds( self, file_parentsObj ):
	    #note we replaced value in 'FILE_PARENT_LFN' with PARENT_FILE_ID in the DBSFile.py
            binds = {}
            if type(file_parentsObj) == type ({}):
            	binds = {
			'FILE_PARENT_ID' : file_parentsObj['FILE_PARENT_ID'],
			'THIS_FILE_ID' : file_parentsObj['THIS_FILE_ID'],
			'PARENT_FILE_ID' : file_parentsObj['FILE_PARENT_LFN']
                 }

            elif type(file_parentsObj) == type([]):
               binds = []
               for item in file_parentsObj:
                   binds.append({
			'FILE_PARENT_ID' : item['FILE_PARENT_ID'],
			'THIS_FILE_ID' : item['THIS_FILE_ID'],
			'PARENT_FILE_ID' : item['FILE_PARENT_LFN']
 	                })
               return binds


    def execute( self, file_parentsObj, conn=None, transaction=False ):
            binds = self.getBinds( file_parentsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


