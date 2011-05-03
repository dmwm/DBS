#!/usr/bin/env python
"""
This module provides business object class to test the speed of dbs w/o access any data. 
"""

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
from sqlalchemy import exceptions

class DBSDoNothing:
    """
    DBSDoNothing business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.lst = daofactory(classname="DoNothing.List")

    def listNone(self):
        try:
            conn=self.dbi.connection()
            result= self.lst.execute(conn)
            return result
        except Exception, ex:
            raise ex
        finally:
            conn.close()
