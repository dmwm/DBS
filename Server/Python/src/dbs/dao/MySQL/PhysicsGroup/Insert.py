#!/usr/bin/env python
""" DAO Object for PhysicsGroups table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/02/05 21:00:46 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner
	    
            self.sql = """INSERT INTO %sPHYSICS_GROUPS ( PHYSICS_GROUP_ID, PHYSICS_GROUP_NAME, PHYSICS_GROUP_CONVENER) VALUES (:physicsgroupid, :physicsgroupname, :physicsgroupconvener)""" % (self.owner)

    def getBinds_delme( self, physics_groupsObj ):
            binds = {}
            if type(physics_groupsObj) == type ('object'):
            	binds = {
			'physicsgroupid' : physics_groupsObj['physicsgroupid'],
			'physicsgroupname' : physics_groupsObj['physicsgroupname'],
			'physicsgroupconvener' : physics_groupsObj['physicsgroupconvener'],
                 }

            elif type(physics_groupsObj) == type([]):
               binds = []
               for item in physics_groupsObj:
                   binds.append({
 	                'physicsgroupid' : item['physicsgroupid'],
 	                'physicsgroupname' : item['physicsgroupname'],
 	                'physicsgroupconvener' : item['physicsgroupconvener'],
 	                })
               return binds


    def execute( self, physics_groupsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( physics_groupsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


