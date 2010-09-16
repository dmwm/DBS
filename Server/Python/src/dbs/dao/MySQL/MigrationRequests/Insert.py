""" DAO Object for MigrationRequests table """ 
""" DAO Object for MigrationRequests table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/04/22 08:05:10 akhukhun Exp $ "

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
(MIGRATION_ID, MIGRATION_URL, MIGRATION_DATASET, MIGRATION_STATUS) 
VALUES(:migration_id, :migration_url, :migration_dataset, :migration_status)
""" % self.owner

    def execute(self, conn, daoinput, transaction = False):
        """
        inputdict must be validated to have the following keys:
	:migration_id, :migration_url, :migration_dataset, :migration_status
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)

