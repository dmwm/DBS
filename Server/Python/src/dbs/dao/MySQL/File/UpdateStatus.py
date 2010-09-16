#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.1 2010/03/03 22:35:53 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.File.UpdateStatus import UpdateStatus as OraFileUpdateStatus

class UpdateStatus(OraFileUpdateStatus):
    pass
