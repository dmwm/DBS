#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
__revision__ = "$Id: UpdateStats.py,v 1.2 2010/02/17 22:31:31 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.Block.List import List as OraBlockList


from dbs.dao.Oracle.Block.UpdateStats import UpdateStats as OraUpdateStats
class UpdateStats(OraUpdateStats):
    pass

