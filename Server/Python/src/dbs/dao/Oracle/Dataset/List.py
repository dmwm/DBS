#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2009/11/27 09:55:03 akhukhun Exp $"
__version__ = "$Revision: 1.9 $"

def op(pattern):
    """ returns 'like' if pattern includes '%' and '=' otherwise"""
    if pattern.find("%") == -1:
        return '='
    else:
        return 'like'

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE,
        D.DATASET_TYPE_ID, DP.DATASET_TYPE,  
        D.PHYSICS_GROUP_ID, PH.PHYSICS_GROUP_NAME
FROM %sDATASETS D 
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
""" % ((self.owner,)*3)

    def formatDict(self, result):
        dictOut = []
        for r in result:
            #descriptions = map(lambda x: str(x).lower(), r.keys)
            descriptions = map(lambda x: str(x), r.keys)

            for i in r.fetchall():
                idict = dict(zip(descriptions, i)) 
                physicsgroupdo = {"PHYSICS_GROUP_ID":idict["PHYSICS_GROUP_ID"],
                              "PHYSICS_GROUP_NAME":idict["PHYSICS_GROUP_NAME"]}
                datasettypedo = {"DATASET_TYPE_ID":idict["DATASET_TYPE_ID"],
                             "DATASET_TYPE":idict["DATASET_TYPE"]}
                idict.update({
                               "PHYSICS_GROUP_DO":physicsgroupdo,
                               "DATASET_TYPE_DO":datasettypedo,
                               })
                idict.pop("PHYSICS_GROUP_ID")
                idict.pop("PHYSICS_GROUP_NAME")
                idict.pop("DATASET_TYPE_ID")
                idict.pop("DATASET_TYPE")
                dictOut.append(idict) 
            r.close()
        return {"result":dictOut} 

    def execute(self, dataset="", conn = None, transaction = False):
        """
        dataset key must be of /a/b/c pattern
        """	
        sql = self.sql
        if not dataset:
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += " WHERE D.DATASET %s :dataset" % op(dataset)
            binds = {"dataset":dataset}
            result = self.dbi.processData(sql, binds, conn, transaction)

        return self.formatDict(result)
        #return [dict(r) for r in result[0].fetchall()]
