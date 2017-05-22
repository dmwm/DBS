#!/usr/bin/env python
""" DBS utility class """

import cherrypy 
import cjson
from time import time
import getpass

from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class dbsUtils:
    """dbsUtils class provides time, client names, etc functions."""

    def __init__(self):
	pass

    def getTime(self):
	return time()

    def getCreateBy(self):
        user = ''
        try:
            user = cherrypy.request.user.get('login')
            #in case a user does not assocate his dn with his HN account, name will be a empty string. 
            if not user:
                user = cherrypy.request.user.get('dn')
                if not user:
                    user = getpass.getuser()
        except AttributeError:
            user = getpass.getuser()
        return user

    def getModifiedBy(self):
	return self.getCreateBy()		

    def decodeLumiIntervals(self, lumi_list):
	"""lumi_list must be of one of the two following formats: 
	   '[[a,b], [c,d],' or 
	   [a1, a2, a3] """
	errmessage = "lumi intervals must be of one of the two following formats: '[[a,b], [c,d], ...],' or [a1, a2, a3 ...] "

        if isinstance(lumi_list, str):
            try:
                lumi_list = cjson.decode(lumi_list)
            except:
                dbsExceptionHandler("dbsException-invalid-input2", "invalid lumi format", None, "Could not decode the input lumi_list: %s" % lumi_list)

        if not isinstance(lumi_list, list):
            dbsExceptionHandler("dbsException-invalid-input2", "invalid lumi input", None, errmessage)

        #check only the first element... in case [1, '2', '3'] is passed, exception will not be raised here.
        if len(lumi_list)==0 or isinstance(lumi_list[0], int):
            return lumi_list
        
        elif isinstance(lumi_list[0], list):
            result = []
            resultext = result.extend
            for lumiinterval in lumi_list:
                if not isinstance(lumiinterval, list) or len(lumiinterval) != 2:
                    dbsExceptionHandler("dbsException-invalid-input2", "invalid lumi input", None, errmessage)
                resultext(list(range(lumiinterval[0], lumiinterval[1]+1)))
            result = list(set(result)) #removes the dublicates, no need to sort
            return result
        
        else: 
            dbsExceptionHandler("dbsException-invalid-input2", 'invalid lumi format', None, \
                                     'Unsupported lumi format: %s. %s' % (lumi_list, errmessage))

def jsonstreamer(func):
    """JSON streamer decorator"""
    def wrapper (self, *args, **kwds):
        gen  = func (self, *args, **kwds)
        yield "["
        firstItem  = True
        for item in gen:
            if not firstItem:
                yield ","
            else:
                firstItem = False
            yield cjson.encode(item)
        yield "]"
    return wrapper
