#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: List.py,v 1.3 2010/03/02 20:18:05 yuyi Exp $"
__version__ = "$Revision: 1.3 $"

from dbs.dao.Oracle.Dataset.List import List as OraDatasetList

class List(OraDatasetList):
        pass

