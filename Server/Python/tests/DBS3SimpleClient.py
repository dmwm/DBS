#!/usr/bin/env python
"""
Very simple dbs3 client:
python dbs3Client.py GET primds 
python dbs3Client.py GET primds/qcd_20_30 
python dbs3Client.py GET primds?primdsname=qcd*
python dbs3Client.py PUT primds '{"primdsname":"QCDTEST_700_800", \ 
                                  "primdstype":"mc"}" 
"""

__revision__ = "$Id: DBS3SimpleClient.py,v 1.1 2009/10/27 17:28:05 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import sys
import json
import urllib2

def call(url, method = "GET", data = ""):
    """Very simple client function""" 
    header = {"Accept": "application/json"}
    opener = urllib2.build_opener()

    if method == 'GET':
        req = urllib2.Request(url = url, headers = header)
    elif method == 'PUT':
        header['Content-Type'] = 'application/json'
        req = urllib2.Request(url = url, data = data, headers = header)
        req.get_method = lambda: 'PUT'
    data = opener.open(req)
    ddata = json.JSONDecoder().decode(data.read())
    return json.dumps(ddata, sort_keys = True, indent = 2)


if __name__ == "__main__":
    URLBASE = "http://localhost:8585/dbs3/"
    if sys.argv[1] == "GET":
        print call(URLBASE + sys.argv[2], sys.argv[1])
    elif sys.argv[1] == "PUT":
        call(URLBASE + sys.argv[2], sys.argv[1], sys.argv[3])
    else:
        print "unknown verb", sys.argv[1]
