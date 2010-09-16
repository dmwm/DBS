#!/usr/bin/env python
""" DAO Object for AssociatedFiles table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:18 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

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


    def execute( self, associated_filesObj, conn, transaction=False ):
	    if not conn:
		raise Exception("dbs/dao/Oracle/AssociatedFile/Insert expect db connection from upper layer.")
            ##binds = self.getBinds( associated_filesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


