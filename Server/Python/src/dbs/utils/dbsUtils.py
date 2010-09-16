#!/usr/bin/env python
""" DBS utility class """

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: dbsUtils.py,v 1.2 2009/12/18 17:48:07 yuyi Exp $ "

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
	return getCreatedBy()		

