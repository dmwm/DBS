#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.10 2010/03/08 23:12:49 afaq Exp $"
__version__ = "$Revision: 1.10 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDSType List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT PDT.PRIMARY_DS_TYPE_ID, PDT.PRIMARY_DS_TYPE AS DATA_TYPE
FROM %sPRIMARY_DS_TYPES PDT 
""" % (self.owner)

    def execute(self, conn, dsType = "", dataset="", transaction = False):
        """
        Lists all primary dataset types if no user input is provided.
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/PrimaryDSType/List expects db connection from up layer.")
        sql = self.sql
        if not dsType  and not dataset:
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        elif dsType and dataset in ("", None):
            sql += "WHERE PDT.PRIMARY_DS_TYPE = :primdstype" 
            binds = {"primdstype":dsType}
            result = self.dbi.processData(sql, binds, conn=conn, transaction=transaction)
	elif dataset and dsType in ("", None):
	    op = ("=", "like")["%" in dataset]
	    sql += "JOIN PRIMARY_DATASETS PDS on PDS.PRIMARY_DS_ID = PDT.PRIMARY_DS_TYPE_ID \
	            JOIN DATASETS DS ON DS.PRIMARY_DS_ID = PDS.PRIMARY_DS_ID \"\
	            WHERE DS.DATASET %s :dataset;"  %op
	    binds={"dataset":dataset}
	    result = self.dbi.processData(sql, binds, conn=conn, transaction=transaction)
	else:
	    raise Exception("Wrong user input for dao Primary_DS_TYPE List.")
        return self.formatDict(result)
