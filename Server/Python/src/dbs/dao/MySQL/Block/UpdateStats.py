#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
from dbs.dao.Oracle.Block.List import List as OraBlockList


from dbs.dao.Oracle.Block.UpdateStats import UpdateStats as OraUpdateStats
class UpdateStats(OraUpdateStats):
    pass

