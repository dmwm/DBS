from urllib.parse import urlparse


class Socks5Proxy(object):
    """Socks5 Proxy Plugin for pycurl
    proxy_url format socks5://username:passwd@hostname:port"""
    def __init__(self, proxy_url):
        parsed_url = urlparse(proxy_url)
        self._proxy_hostname = parsed_url.hostname
        self._proxy_port = parsed_url.port
        self._proxy_user = parsed_url.username
        self._proxy_passwd = parsed_url.password

    def configure_proxy(self, curl_object):
        """configure pycurl proxy settings"""
        curl_object.setopt(curl_object.PROXY, self._proxy_hostname)
        curl_object.setopt(curl_object.PROXYPORT, self._proxy_port)
        curl_object.setopt(curl_object.PROXYTYPE, curl_object.PROXYTYPE_SOCKS5)
        if self._proxy_user and self._proxy_passwd:
            curl_object.setopt(curl_object.PROXYUSERPWD, '%s:%s' % (self._proxy_user, self._proxy_port)) 
