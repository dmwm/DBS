# DAO Object for PhysicsGroup table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:30 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO PHYSICS_GROUPS(PHYSICS_GROUP_ID, PHYSICS_GROUP_NAME, PHYSICS_GROUP_CONVENER) VALUES (:physicsgroupid, :physicsgroupname, :physicsgroupconvener);"""

    def getBinds( self, physics_groupsObj ):
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


    def execute( self, physics_groupsObj ):
            binds = self.getBinds(physics_groupsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return