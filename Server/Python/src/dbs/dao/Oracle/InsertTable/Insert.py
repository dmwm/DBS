#!/usr/bin/env python
""" Super Class for all DAO Object that inserts. """

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/28 22:54:02 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class InsertSingle(DBFormatter):
    """ General class for all Inser Dao"""
    def __init__(self, logger, dbi, owner):
	DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner
	
    def executeSingle( self, daoinput, tablename, conn = None, transaction = False):			
	"""build dynamic sql based on daoinput"""
	sql1 = " insert into %s%s( " %(self.owner, tablename)
	sql2 =" values("
	"Now loop over all the input keys. We need to check if all the keys are valid !!!"
	for key in daoinput:
	    sql1 += "%s," %key
	    sql2 += ":%s," %key
	sql = sql1.strip(',') + ') ' + sql2.strip(',') + ' )'
	try:
	    print sql
	    self.dbi.processData(sql, daoinput, conn, transaction)
	except Exception:
	    raise
    

