#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.4 2010/02/15 21:11:18 yuyi Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger

    def increment(self, seqName, conn = None, transaction = False, incCount=1):
        """
        increments the sequence `seqName` by default `Incremented by one`
        and returns its value
        """
	try:
	    #seqTable = "%sS" %seqName
	    seqTable = "%sS" %seqName
	    sql = "select ID from %s" % seqTable
	    result = self.dbi.processData(sql, [], conn, transaction)
	    resultlist = self.formatDict(result)
	    newSeq = resultlist[0]['id']+incCount
	    sql = "UPDATE %s SET ID=:seq_count" % seqTable
	    seqparms={"seq_count" : newSeq}
	    self.dbi.processData(sql, seqparms, conn, transaction)
	    return newSeq
	except:
	    raise
