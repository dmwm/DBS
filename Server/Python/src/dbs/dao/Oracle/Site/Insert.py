#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:23 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sSITES ( SITE_ID, SITE_NAME) VALUES (:siteid, :sitename) % (self.owner) ;"""

    def getBinds_delme( self, sitesObj ):
            binds = {}
            if type(sitesObj) == type ('object'):
            	binds = {
			'siteid' : sitesObj['siteid'],
			'sitename' : sitesObj['sitename'],
                 }

            elif type(sitesObj) == type([]):
               binds = []
               for item in sitesObj:
                   binds.append({
 	                'siteid' : item['siteid'],
 	                'sitename' : item['sitename'],
 	                })
               return binds


    def execute( self, sitesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( sitesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


