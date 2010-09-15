#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.1 2010/02/05 21:00:25 afaq Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger

    def increment(self, seqName, conn = None, transaction = False):
        """
        increments the sequence `seqName` by default `Incremented by`
        and returns its value
        """
        sql = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
