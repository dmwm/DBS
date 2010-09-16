#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.6 2010/02/24 17:36:54 afaq Exp $"
__version__ = "$Revision: 1.6 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger

    def increment(self, seqName, conn = None, transaction = False, incCount=1):
        """
        increments the sequence `seqName` by default `Incremented by`
        and returns its value
	incCount: is UNUSED variable in Oracle implementation
        """
        sql = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
