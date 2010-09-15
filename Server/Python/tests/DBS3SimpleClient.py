#!/usr/bin/env python
"""
Very simple dbs3 client:
"""

__revision__ = "$Id: DBS3SimpleClient.py,v 1.2 2009/10/28 14:52:48 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import sys
import json
import urllib2

class DBS3Client:
    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.header =  {"Accept": "application/json"}
        self.opener =  urllib2.build_opener()
        
    def get(self, apiurl):
        "method for GET verb"
        url = self.baseurl + apiurl
        req = urllib2.Request(url = url, headers = self.header)
        data = self.opener.open(req)
        ddata = json.JSONDecoder().decode(data.read())
        return json.dumps(ddata, sort_keys = True, indent = 2)
    
    def put(self, apiurl, indata):
        """method for PUT verb"""
        url = self.baseurl + apiurl
        header = self.header
        header['Content-Type'] = 'application/json'
        endata = json.dumps(indata)
        req = urllib2.Request(url = url, data = endata, headers = header)
        req.get_method = lambda: 'PUT'
        self.opener.open(req)


if __name__ == "__main__":
    URLBASE = "http://localhost:8585/dbs3/"
    CLI = DBS3Client(URLBASE)
    print CLI.get(sys.argv[1])
