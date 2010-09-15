#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:23 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, VERSION) VALUES (:releaseversionid, :version) % (self.owner) ;"""

    def getBinds_delme( self, release_versionsObj ):
            binds = {}
            if type(release_versionsObj) == type ('object'):
            	binds = {
			'releaseversionid' : release_versionsObj['releaseversionid'],
			'version' : release_versionsObj['version'],
                 }

            elif type(release_versionsObj) == type([]):
               binds = []
               for item in release_versionsObj:
                   binds.append({
 	                'releaseversionid' : item['releaseversionid'],
 	                'version' : item['version'],
 	                })
               return binds


    def execute( self, release_versionsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( release_versionsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


