#!/usr/bin/env python
""" DBS utility class """

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: dbsUtils.py,v 1.4 2010/08/09 10:59:14 akhukhun Exp $ "

import cjson
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


    def decodeLumiIntervals(self, lumi_list):
	"""lumi_list must be of one of the two following formats: 
	   '[[a,b], [c,d],' or 
	   [a1, a2, a3] """

	errmessage = " lumi intervals must be of one of the two following formats: '[[a,b], [c,d], ...],' or [a1, a2, a3 ...] "
	try:
	    lumi_list = cjson.decode(lumi_list)
	except:
	    raise Exception("Could not decode the input lumilist: %s" % lumi_list)
	else:
	    assert type(lumi_list) == list, errmessage
	    #check only the first element... in case [1, '2', '3'] is passed, exception will not be raised here.
	    if type(lumi_list[0]) == int:
		return lumi_list 
	    elif type(lumi_list[0]) == list:
		result = []
		resultext = result.extend
		for lumiinterval in lumi_list:
		    assert type(lumiinterval) == list, errmessage
		    assert len(lumiinterval) == 2, errmessage
		    resultext(range(lumiinterval[0], lumiinterval[1]+1))
		result = list(set(result)) #removes the dublicates, no need to sort
		return result
	    else: 
		raise Exception('Unsupported lumi format: %s. %s' % (lumi_list, errmessage))

