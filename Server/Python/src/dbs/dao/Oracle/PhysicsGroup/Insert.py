#!/usr/bin/env python
""" DAO Object for PhysicsGroups table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2010/08/09 18:22:07 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    
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


    def execute( self, conn, physics_groupsObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/PhysicsGroup/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, physics_groupsObj, conn, transaction)
	return


