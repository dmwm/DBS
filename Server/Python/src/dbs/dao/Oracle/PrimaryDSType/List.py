#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.12 2010/03/19 19:18:57 yuyi Exp $"
__version__ = "$Revision: 1.12 $"


from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsException import dbsException,dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
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
SELECT DISTINCT PDT.PRIMARY_DS_TYPE_ID, PDT.PRIMARY_DS_TYPE AS DATA_TYPE
FROM %sPRIMARY_DS_TYPES PDT 
""" % (self.owner)

    def execute(self, conn, dsType = "", dataset="", transaction = False):
        """
        Lists all primary dataset types if no user input is provided.
        """
	if not conn:
	    dbsExceptionHandler("dbsException-dao", "%s. PrimaryDSType/List expects db connection from upper\
                layer."%dbsExceptionCode["dbsException-dao"])
        sql = self.sql
        binds={}
        if not dsType  and not dataset:
            pass
        elif dsType and dataset in ("", None, '%'):
            op = ("=", "like")["%" in dsType]
            sql += "WHERE PDT.PRIMARY_DS_TYPE %s :primdstype"%op 
            binds = {"primdstype":dsType}
	elif dataset and dsType in ("", None, '%'):
	    op = ("=", "like")["%" in dataset]
	    sql += "JOIN %sPRIMARY_DATASETS PDS on PDS.PRIMARY_DS_TYPE_ID = PDT.PRIMARY_DS_TYPE_ID \
	            JOIN %sDATASETS DS ON DS.PRIMARY_DS_ID = PDS.PRIMARY_DS_ID \
	            WHERE DS.DATASET %s :dataset"  %(self.owner, self.owner, op)
	    binds={"dataset":dataset}
        elif dataset and dsType:
            op = ("=", "like")["%" in dsType]
            op1 = ("=", "like")["%" in dataset]
            sql += "JOIN %sPRIMARY_DATASETS PDS on PDS.PRIMARY_DS_TYPE_ID = PDT.PRIMARY_DS_TYPE_ID \
                    JOIN %sDATASETS DS ON DS.PRIMARY_DS_ID = PDS.PRIMARY_DS_ID \
                    WHERE DS.DATASET %s :dataset and PDT.PRIMARY_DS_TYPE %s :primdstype" \
                    %(self.owner, self.owner,op1, op)
            binds = {"primdstype":dsType, "dataset":dataset}
	else:
	    dbsExceptionHandler('dbsException-invalid-input', "%s DAO Primary_DS_TYPE List accepts no input, or\
            dataset,primary_ds_type as input." %dbsExceptionCode["dbsException-invalid-input"])
        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        if len(cursors) == 0 :
            return []
        else:
            return self.formatCursor(cursors[0]) 
