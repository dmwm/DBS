#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/02/11 19:39:35 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.Site.List import List as OraSiteList

class List(OraSiteList):
        pass

