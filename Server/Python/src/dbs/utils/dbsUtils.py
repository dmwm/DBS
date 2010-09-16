#!/usr/bin/env python
""" DBS utility class """

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: dbsUtils.py,v 1.3 2009/12/23 20:30:40 yuyi Exp $ "

from time import time

class dbsUtils:
    """dbsUtils class provides time, client names, etc functions."""

    def __init__(self):
	pass

    def getTime(self):
	return time()

    def getCreateBy(self):
	return "Client Name"

    def getModifiedBy(self):
	return self.getCreateBy()		

