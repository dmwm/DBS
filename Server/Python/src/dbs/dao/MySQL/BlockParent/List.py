#!/usr/bin/env python
"""
This module provides BlockParent.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/04/16 21:57:30 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.BlockParent.List import List as OraBlockParentList

class List(OraBlockParentList):
    pass

