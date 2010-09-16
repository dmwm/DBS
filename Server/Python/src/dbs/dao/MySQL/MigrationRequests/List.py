#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/06/25 18:50:47 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.MigrationRequests.List import List as OraMigList

class List(OraMigList):
    pass
		   
