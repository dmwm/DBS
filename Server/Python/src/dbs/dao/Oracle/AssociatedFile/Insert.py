#!/usr/bin/env python
""" DAO Object for AssociatedFiles table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/28 23:07:59 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sASSOCIATED_FILES ( ASSOCATED_FILE_ID, THIS_FILE_ID, ASSOCATED_FILE) VALUES (:assocatedfileid, :thisfileid, :assocatedfile)""" % (self.owner)

    def getBinds_delme( self, associated_filesObj ):
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


    def execute( self, associated_filesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( associated_filesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


