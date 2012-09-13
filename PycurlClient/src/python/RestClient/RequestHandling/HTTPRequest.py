from RestClient.RequestHandling.HTTPResponse import HTTPResponse
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

import pycurl
import urllib

try:
    from cStringIO import StringIO
except ImportError:
    import StringIO

class HTTPRequest(object):
    supported_methods = {'GET'    : {pycurl.HTTPGET : True},
                         'POST'   : {pycurl.POST : True},
                         'PUT'    : {pycurl.UPLOAD : True},#pycurl.PUT has been deprecated
                         'DELETE' : {pycurl.CUSTOMREQUEST : 'DELETE'}}

    def __init__(self, method, url, api, params, data, request_headers={}, additional_curl_options={}):
        method = method.upper()
        self._curl_options = dict(additional_curl_options) ### copy dict since mutables are shared between instances
        request_headers = dict(request_headers) ### copy dict since mutables are shared between instances

        try:
            self._curl_options.update(self.supported_methods[method])
        except KeyError as ke:
            raise NotImplementedError("HTTP method %s has not been implemented yet." % (ke))

        if not params:
            self._curl_options[pycurl.URL] = ("%s/%s") % (url, api)
        else:
            self._curl_options[pycurl.URL] = ("%s/%s?%s") % (url, api, urllib.urlencode(params))

        if method == 'POST':
            self._curl_options[pycurl.POSTFIELDS] = data
            if not data: ###for zero-byte post this is a mandatory option
                self._curl_options[pycurl.POSTFIELDSIZE] = 0
            ### pycurl will automatically set content-length header using strlen()

        elif method == 'PUT':
            data_fp = StringIO(data)
            content_length = len(data)
            self._curl_options[pycurl.READFUNCTION] = data_fp.read
            self._curl_options[pycurl.INFILESIZE] = content_length
            ### set content-length header to ensure performant cherrypy reads
            request_headers['Content-Length'] = str(content_length)

        self._curl_options[pycurl.HTTPHEADER] = ["%s: %s" % (key, value) for key, value in request_headers.iteritems()]

    def __call__(self, curl_object):
        for key, value in self._curl_options.iteritems():
            curl_object.setopt(key, value)

        http_response  = HTTPResponse()
        curl_object.setopt(pycurl.HEADERFUNCTION, http_response.pycurl_header_function)
        curl_object.setopt(pycurl.WRITEFUNCTION, http_response.pycurl_write_function)

        curl_object.perform()

        http_code = curl_object.getinfo(pycurl.HTTP_CODE)

        if http_code < 200 or http_code >=300:
            effective_url = curl_object.getinfo(pycurl.EFFECTIVE_URL)
            raise HTTPError(effective_url, http_code, http_response.msg, http_response.raw_header, http_response.body)

        return http_response
