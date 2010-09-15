#!/usr/bin/env python
"""
Very simple dbs3 client:
"""

__revision__ = "$Id: DBS3SimpleClient.py,v 1.4 2009/11/03 16:42:25 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

import sys
import json
import urllib2

class DBS3Client:
    def __init__(self, baseurl):
        self.baseurl = baseurl
        self.header =  {"Accept": "application/json"}
        self.opener =  urllib2.build_opener()
        
    def get(self, apiurl, logfile=""):
        "method for GET verb"
        url = self.baseurl + apiurl
        req = urllib2.Request(url = url, headers = self.header)
        data = self.opener.open(req)
        ddata = json.JSONDecoder().decode(data.read())
	if not logfile == "":	
	    f=open(logfile, "w")
	    f.write(json.dumps(ddata))
	    f.close()
        return json.dumps(ddata, sort_keys = True, indent = 4)
    
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
    if len(sys.argv)==2:
	print CLI.get(sys.argv[1])
    else:
	print CLI.get(sys.argv[1], sys.argv[2])

