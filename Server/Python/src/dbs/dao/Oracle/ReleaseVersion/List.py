#!/usr/bin/env python
"""
This module provides ReleaseVersion.List data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2010/08/09 18:43:08 yuyi Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    ReleaseVersion List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT RV.RELEASE_VERSION FROM %sRELEASE_VERSIONS RV 
""" % (self.owner)

    def execute(self, conn, releaseVersion='', dataset='', transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/ReleaseVersion/List expects db connection from upper layer.")
        sql = self.sql
	binds={}
	if releaseVersion and not dataset:
            op = ("=", "like")["%" in releaseVersion]
	    sql += "WHERE RV.RELEASE_VERSION %s :release_version" %op 
	    binds = {"release_version":releaseVersion}
        elif dataset and not releaseVersion:
            sql += "join %sOUTPUT_MODULE_CONFIGS OMF on OMF.RELEASE_VERSION_ID =RV.RELEASE_VERSION_ID\
            join %sDATASET_OUTPUT_MOD_CONFIGS do on do.OUTPUT_MOD_CONFIG_ID=OMF.OUTPUT_MOD_CONFIG_ID\
            join %sDATASETS d on d.DATASET_ID= do.DATASET_ID\
            WHERE d.DATASET=:dataset" %((self.owner,)*3)
            binds={"dataset":dataset}
        elif dataset and releaseVersion:
            op = ("=", "like")["%" in releaseVersion]
            sql += "join %sOUTPUT_MODULE_CONFIGS OMF on OMF.RELEASE_VERSION_ID =RV.RELEASE_VERSION_ID\
            join %sDATASET_OUTPUT_MOD_CONFIGS do on do.OUTPUT_MOD_CONFIG_ID=OMF.OUTPUT_MOD_CONFIG_ID\
            join %sDATASETS d on d.DATASET_ID= do.DATASET_ID\
            WHERE d.DATASET=:dataset and RV.RELEASE_VERSION %s :release_version" \
            %(self.owner, self.owner, self.owner, op)
            binds={"dataset":dataset, "release_version":releaseVersion}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
