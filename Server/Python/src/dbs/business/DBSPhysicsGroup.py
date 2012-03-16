#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Physics Groups.
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSPhysicsGroup:
    """
    Physics Group business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.pglist = daofactory(classname="PhysicsGroup.List")


    def listPhysicsGroups(self, physics_group_name=""):
        """
        Returns all physics groups if physics group names are not passed.
        """
        if type(physics_group_name) is not str:
            dbsExceptionHandler('dbsException-invalid-input',
                'physics group name given is not valid : %s' %
                 physics_group_name)
        conn = self.dbi.connection()
        try:
            result = self.pglist.execute(conn, physics_group_name)
            return result
        finally:
            if conn:
                conn.close()
