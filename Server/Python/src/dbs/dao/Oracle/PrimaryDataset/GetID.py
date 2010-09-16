#!/usr/bin/env python
"""
This module provides PrimaryDataset.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.5 2010/06/23 21:21:25 afaq Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    PrimaryDataset GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME
FROM %sPRIMARY_DATASETS P 
""" % (self.owner)

    def execute(self, conn, name, transaction = False):
        """
        returns id for a given primary dataset name
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/PrimaryDataset/GetID expects db connection from upper layer.")
        sql = self.sql
        sql += "WHERE P.PRIMARY_DS_NAME = :primarydataset" 
        binds = {"primarydataset":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "PrimaryDataset %s does not exist" % name
        return plist[0]["primary_ds_id"]
