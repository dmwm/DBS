#!/usr/bin/env python
"""
This module provides MigrationRequests.FindPendingRequest data access object.
"""
__revision__ = "$Id: FindPendingRequest.py,v 1.2 2010/08/18 18:48:59 yuyi Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.MigrationRequests.FindPendingRequest import FindPendingRequest as OraMigFindPendingRequest

class FindPendingRequest(OraMigFindPendingRequest):
    pass
		   
