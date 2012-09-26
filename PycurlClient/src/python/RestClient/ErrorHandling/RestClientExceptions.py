class ClientAuthException(Exception):
    def __init__(self, msg):
        self._msg = msg
        super(ClientAuthException, self).__init__(self, "ClientAuthException %s" % self._msg)

    def __repr__(self):
        return ('%s %r' % (self.__class__.__name__, self._msg))

    def __str__(self):
        return repr(self._msg)

class HTTPError(Exception):
    def __init__(self, url, code, msg, header, body):
        self.url = url
        self.code = code
        self.msg = msg
        self.header = header
        self.body = body
        super(HTTPError, self).__init__(self, "HTTPError %d" % self.code)

    def __repr__(self):
        return ('%s %r' % (self.__class__.__name__, self.code))

    def __str__(self):
        return ('HTTP Error %d: %s' % (self.code, self.msg))
