#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.1 2009/10/15 15:29:58 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """

    def increment(self, seqName, conn=None, transaction=False):
        """
        increments the sequence `seqName` by one
        and returns its value
        """
        sql = "select %s.nextval as val from dual" % (seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']

    def currentid(self, seqName):
        """
        returns current value of sequence `seqName`
        not working for now.
        """
        sql = "select %s.currval as val from dual" % (seqName)
        result = self.dbi.processData(sql)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
