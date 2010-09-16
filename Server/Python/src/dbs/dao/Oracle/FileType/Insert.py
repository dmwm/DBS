#!/usr/bin/env python
""" DAO Object for FileTypes table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2010/08/18 19:47:29 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    
            self.sql = """INSERT INTO %sFILE_DATA_TYPES ( FILE_TYPE_ID, FILE_TYPE) VALUES (:filetypeid, :filetype)""" % (self.owner)

    def getBinds_delme( self, file_typesObj ):
            binds = {}
            if type(file_typesObj) == type ('object'):
            	binds = {
			'filetypeid' : file_typesObj['filetypeid'],
			'filetype' : file_typesObj['filetype'],
                 }

            elif type(file_typesObj) == type([]):
               binds = []
               for item in file_typesObj:
                   binds.append({
 	                'filetypeid' : item['filetypeid'],
 	                'filetype' : item['filetype'],
 	                })
               return binds


    def execute( self, conn, file_typesObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/FileType/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


