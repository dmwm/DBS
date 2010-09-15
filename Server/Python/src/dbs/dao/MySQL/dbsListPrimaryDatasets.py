#!/usr/bin/env python

class dbsListPrimaryDatasets:
    def __init__(self, primdsname, schemaowner):
	if (primdsname.find("%")==-1 and primdsname.find("_")==-1): cond="="
	else: cond="like"
	
	self.sql="""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME, P.PRIMARY_DS_TYPE_ID, 
       PT.PRIMARY_DS_TYPE, P.CREATION_DATE, P.CREATE_BY
FROM %sPRIMARY_DATASETS P
JOIN %sPRIMARY_DS_TYPES PT
ON PT.PRIMARY_DS_TYPE_ID=P.PRIMARY_DS_TYPE_ID
WHERE P.PRIMARY_DS_NAME %s :primdsname
""" % (schemaowner, schemaowner, cond)
	self.binds= {"primdsname":primdsname}



