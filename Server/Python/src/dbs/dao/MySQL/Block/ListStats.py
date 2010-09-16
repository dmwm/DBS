#!/usr/bin/env python
"""
This module provides Block.ListStats data access object.
Block parameters based on current conditions at DBS, are listed by this DAO
"""
__revision__ = "$Id: ListStats.py,v 1.2 2010/02/11 19:39:29 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.Block.ListStats import List as OraBlockListStats

class ListStats(OraBlockListStats):
        pass
	

