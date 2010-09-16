""" DAO Object for MigrationBlocks table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/06/29 19:28:46 afaq Exp $ "

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
INSERT INTO %sMIGRATION_BLOCKS
(MIGRATION_BLOCK_ID, MIGRATION_REQUEST_ID, MIGRATION_BLOCK, MIGRATION_ORDER, MIGRATION_STATUS, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
VALUES(:migration_block_id, :migration_request_id, :migration_block, :migration_order, :migration_status, :creation_date, :create_by, :last_modification_date, :last_modified_by)
""" % self.owner

    def execute(self, conn, daoinput, transaction = False):
        """
	insert into MIGRATION_BLOCKS
        """

	print "what the heck................................."
	print self.sql
	print daoinput[0]
	print "what the heck................................."
        self.dbi.processData(self.sql, daoinput, conn, transaction)
