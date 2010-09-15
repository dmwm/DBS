#!/usr/bin/env python
""" DBS utility class """

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: dbsUtils.py,v 1.1 2009/12/16 20:12:23 yuyi Exp $ "

from time import time

class dbsUtils:
    """dbsUtils class provides time, client names, etc functions."""

    def __init__(self, logger):
	pass

    def getTime(self):
	return time()

    def getCreateBy(self):
	return "Client Name"

    def getModifiedBy(self):
	return getCreatedBy()		

