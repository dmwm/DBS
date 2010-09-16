#!/usr/bin/env python
"""
This class provide the next avaliable pending request. 

"""
__revision__ = "$Id: FindPendingRequest.py,v 1.2 2010/08/25 21:41:52 afaq Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

class FindPendingRequest(DBFormatter):
    """
    MigrationRequest FindPendingRequest DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
#	self.sql ="""select MIGRATION_REQUEST_ID, MIGRATION_URL from %sMIGRATION_REQUESTS where MIGRATION_STATUS=0
# 	             and LAST_MODIFIED_BY=0 
#                     and MIGRATION_REQUEST_ID=(select min(MIGRATION_REQUEST_ID) FROM %sMIGRATION_REQUESTS 
#                     WHERE MIGRATION_STATUS=0 )  for update """ % ((self.owner,)*2)
		     
        self.sql ="""select MIGRATION_REQUEST_ID, MIGRATION_URL from %sMIGRATION_REQUESTS where MIGRATION_STATUS=0
		     and MIGRATION_REQUEST_ID=(select min(MIGRATION_REQUEST_ID) FROM %sMIGRATION_REQUESTS 
		     WHERE MIGRATION_STATUS=0 )  for update """ % ((self.owner,)*2)
									       
    def execute(self, conn, transaction=False):
        """
	find the pending request
        """

        binds = {}
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	#just a note: formatDict changes the column name to lower case
        result = self.formatDict(result)
	if len(result) == 0 :
	    return []
	if result[0]["migration_request_id"] in ('', None) :
	    return []
        return result
