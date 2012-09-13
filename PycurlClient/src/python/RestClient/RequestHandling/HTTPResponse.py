try:
    from cStringIO import StringIO
except ImportError:
    import StringIO

class HTTPResponse(object):
    def __init__(self):
        self._response_header = StringIO()
        self._response_body = StringIO()

    def __parse_header(self):
        self._header_dict = {}
        for header in self._response_header.getvalue().split('\r\n'):
            if header.startswith('HTTP'):
                self._version, self._code, self._msg = header.split(' ', 2)
            elif header != "":
                self._header_dict.update(dict([header.split(':',1)]))

    @property
    def body(self):
        """Returns raw body received from server"""
        return self._response_body.getvalue()

    @property
    def fp_body(self):
        """Returns body file pointer equivalent from StringIO.
        For example to read data directly using json.load(fp)"""
        self._response_body.seek(0)
        return self._response_body

    @property
    def raw_header(self):
        """Returns raw response header received from server"""
        return self._response_header.getvalue()
    
    @property
    def header(self):
        """Returns header dictionary created by parsing raw response header from server"""
        if not hasattr(self, '_header_dict'):
            self.__parse_header()
        return self._header_dict

    @property
    def pycurl_write_function(self):
        """Returns body write function from StringIO.
        To be used as pycurl.WRITEFUNCTION"""
        return self._response_body.write

    @property
    def pycurl_header_function(self):
        """Returns header write function from StringIO.
        To be used as pycurl.HEADERFUNCTION"""
        return self._response_header.write

    @property
    def code(self):
        """Returns http code from server response header"""
        if not hasattr(self,'_code'):
            self.__parse_header()
        return self._code

    @property
    def msg(self):
        """Returns message from server response header"""
        if not hasattr(self,'_msg'):
            self.__parse_header()
        return self._msg

    @property
    def version(self):
        """Returns used HTTP version from server response header"""
        if not hasattr(self,'_version'):
            self.__parse_header()
        return self._version
