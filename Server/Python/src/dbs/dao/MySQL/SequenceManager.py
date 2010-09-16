#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.7 2010/04/22 16:22:13 yuyi Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger

    def increment(self, conn, seqName, transaction = False, incCount=1):
        """
        increments the sequence `seqName` by default `Incremented by one`
        and returns its value
        """
	try:
	    seqTable = "%sS" %seqName
	    tlock = "lock tables %s write" %seqTable
	    self.dbi.processData(tlock, [], conn, transaction)
	    sql = "select ID from %s" % seqTable
	    result = self.dbi.processData(sql, [], conn, transaction)
	    resultlist = self.formatDict(result)
	    newSeq = resultlist[0]['id']+incCount
	    sql = "UPDATE %s SET ID=:seq_count" % seqTable
	    seqparms={"seq_count" : newSeq}
	    self.dbi.processData(sql, seqparms, conn, transaction)
	    tunlock = "unlock tables"
	    self.dbi.processData(tunlock, [], conn, transaction)
	    return newSeq
	except:
	    #FIXME
	    tunlock = "unlock tables"
	    self.dbi.processData(tunlock, [], conn, transaction)
	    raise
