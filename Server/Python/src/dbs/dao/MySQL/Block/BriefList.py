#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: BriefList.py,v 1.1 2010/08/01 19:07:33 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Block.BriefList import BriefList as OraBlockBriefList

class BriefList(OraBlockBriefList):
    pass

