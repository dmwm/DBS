#!/usr/bin/env python
""" DAO Object for AcquisitionEras table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/11/24 10:58:12 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """AcquisitionEras Insert DAO Class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner

        self.sql = \
"""INSERT INTO %sACQUISITION_ERAS 
(ACQUISITION_ERA_ID, ACQUISITION_ERA_NAME, 
CREATION_DATE, CREATE_BY, DESCRIPTION) 
VALUES (:acquisitioneraid, :acquisitioneraname, 
:creationdate, :createby, :description) 
""" % self.owner

    def execute(self, daoinput, conn = None, transaction = False ):
        """
        daoinput must be validated to have the following keys:
        acquisitioneraid, acquisitioneraname, creationdate,
        createby, description
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)


