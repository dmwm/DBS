#!/usr/bin/env python
"""
Very simple dbs3 client:
"""

__revision__ = "$Id: DBS3SimpleClient.py,v 1.8 2009/11/29 11:37:54 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"

import sys
import cjson
import urllib, urllib2

class DBS3Client:
    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.header =  {"Accept": "application/json"}
        self.opener =  urllib2.build_opener()
        
    def get(self, apiurl, params = {}):
        "method for GET verb"
        url = self.baseurl + apiurl
        if not params == {}:
            url = "?".join((url, urllib.urlencode(params, doseq=True)))
        #req = urllib2.Request(url = url, headers = self.header)
        req = urllib2.Request(url = url)
        data = self.opener.open(req)
        #ddata = cjson.decode(data.read())
        #return ddata
        a = data.read()
        data.close()
        return a
    
    def put(self, apiurl, indata):
        """method for PUT verb"""
        url = self.baseurl + apiurl
        header = self.header
        header['Content-Type'] = 'application/json'
        endata = cjson.encode(indata)
        req = urllib2.Request(url = url, data = endata, headers = header)
        req.get_method = lambda: 'POST'
        self.opener.open(req)

if __name__ == "__main__":
    import json
    
    URLBASE = "http://vocms09.cern.ch:8585/dbs3/"
    #URLBASE = "http://vocms09.cern.ch:8989/DBSServlet/"

    url = {"java":"http://vocms09.cern.ch:8989/DBSServlet/",
           "xxpy":"http://localhost/intlxx/",
           "yypy":"http://localhost/intlyy/"}
    CLI = DBS3Client(url[sys.argv[1]])
    res = CLI.get(sys.argv[2])
    print json.dumps(res, sort_keys = True, indent = 4)
    #print res

