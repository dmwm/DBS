#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.8 2010/06/29 19:28:46 afaq Exp $"
__version__ = "$Revision: 1.8 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger

    def increment(self, conn, seqName, transaction = False, incCount=1):
        """
        increments the sequence `seqName` by default `Incremented by`
        and returns its value
	incCount: is UNUSED variable in Oracle implementation
        """

	#FIXME: Do we need to lock the tables here?
	
	if not conn:
	    raise Exception("dbs/dao/Oracle/SequenceManager expects db connection from up layer.")
        sql = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
