""" DAO Object for MigrationRequests table """ 
""" DAO Object for MigrationRequests table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2010/06/24 21:38:52 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """ Migration Insert DAO Class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
"""
INSERT INTO %sMIGRATION_REQUESTS 
(MIGRATION_ID, MIGRATION_URL, MIGRATION_INPUT, MIGRATION_STATUS, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
VALUES(:migration_id, :migration_url, :migration_input, :migration_status, :creation_date, :create_by, :last_modification_date, :last_modified_by)
""" % self.owner

    def execute(self, conn, daoinput, transaction = False):
        """
        inputdict must be validated to have the following keys:
	:migration_id, :migration_url, :migration_input, :migration_status
	:creation_date, :create_by, :last_modification_date, :last_modified_by
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)

