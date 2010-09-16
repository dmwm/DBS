#!/usr/bin/env python
"""
This module provides Site.ListBlockSite data access object.
"""

__revision__ = "$Id: ListBlockSite.py,v 1.1 2010/04/21 19:50:02 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Site.ListBlockSite import ListBlockSite as OraSiteListBlockSite

class ListBlockSite(OraSiteListBlockSite):
        pass

