#!/usr/bin/env python
"""
This module provides business object class to interact with Physics Groups. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS

class DBSPhysicsGroup:
    """
    Physics Group business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.pglist = daofactory(classname="PhysicsGroup.List")


    def listPhysicsGroups(self, physics_group_name=""):
        """
        Returns all physics groups if physics group names are not passed.
        """
	try:
	    conn=self.dbi.connection()
	    result= self.pglist.execute(conn, physics_group_name)
	    return result
        except Exception, ex:
            raise ex
	finally:
	    conn.close()
