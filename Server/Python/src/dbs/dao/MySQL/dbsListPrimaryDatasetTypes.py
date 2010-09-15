#!/usr/bin/env python

class dbsListPrimaryDatasetTypes:
    def __init__(self, typename, schemaowner):
	if (typename.find("%")==-1 and typename.find("_")==-1): cond="="
	else: cond="like"

	self.sql = """
SELECT PT.PRIMARY_DS_TYPE_ID, PT.PRIMARY_DS_TYPE  
FROM %sPRIMARY_DS_TYPES PT
WHERE PT.PRIMARY_DS_TYPE %s :primdstype;
""" % (schemaowner, cond)
	self.binds = {"primdstype": typename}


