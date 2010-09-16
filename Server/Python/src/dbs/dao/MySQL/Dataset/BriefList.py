#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
Lists dataset_parent and output configuration parameters too.
"""
__revision__ = "$Id: BriefList.py,v 1.1 2010/08/01 19:06:38 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Dataset.BriefList import BriefList as OraDatasetBriefList

class BriefList(OraDatasetBriefList):
        pass

