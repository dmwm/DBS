#!/usr/bin/env python
"""
This module provides MigrationRequests.FindPendingRequest data access object.
"""
__revision__ = "$Id: FindPendingRequest.py,v 1.1 2010/08/18 18:46:38 yuyi Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.MigrationRequests.FindPendingRequest import List as OraMigFindPendingRequest

class FindPendingRequest(OraMigFindPendingRequest):
    pass
		   
