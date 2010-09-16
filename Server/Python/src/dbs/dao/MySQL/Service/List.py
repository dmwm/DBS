#!/usr/bin/env python
"""
This module provides Service.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/08/02 20:41:10 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Service.List import List as OraServiceList

class List(OraServiceList):
        pass

