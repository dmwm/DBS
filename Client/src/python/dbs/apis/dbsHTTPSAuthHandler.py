import os, sys, socket
import urllib, urllib2
import httplib

class HTTPSAuthHandler(urllib2.HTTPSHandler):
    """
    HTTPS authentication class to provide path of key/cert files.
    """
    def __init__(self, key=None, cert=None, level=0):
        urllib2.HTTPSHandler.__init__(self, debuglevel=level)
        self.key = key
        self.cert = cert

    def get_connection(self, host, timeout=300):
        if  self.key and self.cert:
            return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)
        return httplib.HTTPSConnection(host)


    def https_open(self, req):
        """
        Overwrite the default https_open.
        """
        return self.do_open(self.get_connection, req)
