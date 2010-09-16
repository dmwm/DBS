#!/usr/bin/env python
"""
This module provides DataTier.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/08/05 14:47:15 yuyi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    DataTier List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT AE.ACQUISTION_ERA_NAME, AE.CREATE_DATE, AE.CREATE_BY, AE.DESCRIPTION   
FROM %sACQUISITION_ERAS AE 
""" % (self.owner)

    def execute(self, conn, acquisitionEra, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/AcquisitionEra/List expects db connection from upper layer.")
        sql = self.sql
	binds={}
	if acquisitionEra:
	    sql += "WHERE AE.ACQUISITION_ERA_NAME = :acquisitionEra" 
	    binds = {"acquisitionEra":acquisitionEra}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
