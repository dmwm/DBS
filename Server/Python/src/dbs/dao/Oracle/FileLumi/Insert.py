#!/usr/bin/env python
""" DAO Object for FileLumis table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/11/17 19:46:30 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    def __init__(self, logger, dbi):
	DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % self.dbi.engine.url.username

	self.sql = """INSERT INTO %sFILE_LUMIS ( FILE_LUMI_ID, RUN_NUM,
	    LUMI_SECTION_NUM, FILE_ID) VALUES (:FILE_LUMI_ID, :RUN_NUM,
	    :LUMI_SECTION_NUM, :FILE_ID) """  % (self.owner)

    def getBinds( self, file_lumisObj ):

	binds = {}
	if type(file_lumisObj) == type ({}):
	    binds = {
			'FILE_LUMI_ID' : file_lumisObj['FILE_LUMI_ID'],
			'RUN_NUM' : file_lumisObj['RUN_NUM'],
			'LUMI_SECTION_NUM' : file_lumisObj['LUMI_SECTION_NUM'],
			'FILE_ID' : file_lumisObj['FILE_ID'] }

	elif type(file_lumisObj) == type([]):
	    binds = []
	    for item in file_lumisObj:
		binds.append({
			'FILE_LUMI_ID' : item['FILE_LUMI_ID'],
			'RUN_NUM' : item['RUN_NUM'],
			'LUMI_SECTION_NUM' : item['LUMI_SECTION_NUM'],
			'FILE_ID' : item['FILE_ID']
		})
	return binds
    def execute( self, file_lumisObj, conn=None, transaction=False ):
	binds = self.getBinds( file_lumisObj )
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


