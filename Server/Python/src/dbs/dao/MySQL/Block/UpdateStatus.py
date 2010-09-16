#!/usr/bin/env python
"""
This module provides Block.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.1 2010/04/21 19:42:36 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Block.UpdateStatus import UpdateStatus as OraBlockUpdateStatus

class UpdateStatus(OraBlockUpdateStatus):
    pass
