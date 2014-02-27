#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class List(DBFormatter):
    """
    MigrationRequest List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT MR.MIGRATION_REQUEST_ID, MR.MIGRATION_URL, 
       MR.MIGRATION_INPUT, MR.MIGRATION_STATUS,
       MR.CREATE_BY, MR.CREATION_DATE,
       MR.LAST_MODIFIED_BY, MR.LAST_MODIFICATION_DATE, MR.RETRY_COUNT
FROM %sMIGRATION_REQUESTS MR
""" % (self.owner)

    def execute(self, conn, migration_url="", migration_input="", create_by="", migration_request_id="", oldest= False, transaction=False):
        """
        Lists all requests if pattern is not provided.
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationRequests/List. Expects db connection from upper layer.")
        sql = self.sql
        binds = {}
	if migration_request_id:
	    sql += " WHERE MR.MIGRATION_REQUEST_ID=:migration_request_id"
	    binds['migration_request_id']=migration_request_id
        elif oldest:
            #FIXME: Need to write the sql.YG
            #current_date = dbsUtils().getTime()
            #we require waiting time for 
            #retry_count=0 is 1 minutes
            #retry_count=1 is 2 minutes
            #retyr_count=2 is 4 minutes

            sql += """
                       WHERE MR.MIGRATION_STATUS=0 
                       or (MR.migration_status=3 and MR.retry_count=0 and MR.last_modification_date <= :current_date-60)    
                       or (MR.migration_status=3 and MR.retry_count=1 and MR.last_modification_date <= :current_date-120)  
                       or (MR.migration_status=3 and MR.retry_count=2 and MR.last_modification_date <= :current_date-240)
                       ORDER BY MR.creation_date
                   """ 
            binds['current_date'] = dbsUtils().getTime()
            #print "time= " + str(binds['current_date'])
        else:    
	    if  migration_url or migration_input or create_by:
		sql += " WHERE "
	    if migration_url:
		sql += " MR.MIGRATION_URL=:migration_url"
		binds['migration_url']=migration_url
	    if  migration_input:
		if migration_url:
		    sql += " AND "
		op = ("=", "like")["%" in migration_input]
		sql += " MR.MIGRATION_INPUT %s :migration_input" % op
		binds['migration_input']=migration_input
	    if create_by:
		if  migration_url or migration_input:
		    sql += " AND "
		sql += " MR.CREATE_BY=:create_by" %create_by
		binds['create_by']=create_by
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
