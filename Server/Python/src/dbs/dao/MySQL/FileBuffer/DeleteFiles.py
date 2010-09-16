#!/usr/bin/env python
"""
This module provides FileBuffer.DeleteFiles data access object.
"""
__revision__ = "$Id: DeleteFiles.py,v 1.1 2010/05/25 21:00:37 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.FileBuffer.DeleteFiles import DeleteFiles as OraFileBufferDeleteFiles

class DeleteFiles(OraFileBufferDeleteFiles):
                pass

