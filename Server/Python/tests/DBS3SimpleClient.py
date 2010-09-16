#!/usr/bin/env python
"""
Very simple dbs3 client:
"""

__revision__ = "$Id: DBS3SimpleClient.py,v 1.6 2009/11/19 17:38:57 akhukhun Exp $"
__version__ = "$Revision: 1.6 $"

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
        req = urllib2.Request(url = url, headers = self.header)
        data = self.opener.open(req)
        ddata = cjson.decode(data.read())
        return ddata
    
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
    
    URLBASE = "http://localhost:8585/dbs3/"
    CLI = DBS3Client(URLBASE)
    if len(sys.argv)==2:
        res = CLI.get(sys.argv[1])
    elif len(sys.argv)==3:
        params = cjson.decode(sys.argv[2])
        res = CLI.get(sys.argv[1], params)
    else: res = {}
    print json.dumps(res, sort_keys = True, indent = 4)

