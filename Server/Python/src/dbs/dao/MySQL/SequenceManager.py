#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.2 2010/02/11 17:31:04 afaq Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger

    def increment(self, seqName, conn = None, transaction = False, seqCount=1):
        """
        increments the sequence `seqName` by default `Incremented by one`
        and returns its value
        """
	try:
	    sql = "UPDATE sequence SET %s=LAST_INSERT_ID(:seq_count)" % (seqName)
	    seqparms={"seq_count" : seqCount}
	    self.dbi.processData(sql, seqparms, conn, transaction)
    
	    sql = "select LAST_INSERT_ID() as val"
	    result = self.dbi.processData(sql, conn=conn, transaction=transaction)
	    resultlist = self.formatDict(result)
	    return resultlist[0]['val']
	except:
	    raise
