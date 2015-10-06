#!/usr/bin/env python
import xmlrpclib
import cPickle

class StatsXMLRPCClient(object):
    def __init__(self, host="localhost", port=9876):
        self.stats_server = xmlrpclib.ServerProxy('http://%s:%s' % (host, port))

    def send(self, stats):
        self.stats_server.add_stats(stats)
        
    def shutdown_server(self):
        self.stats_server.shutdown()

class StatsPipeClient(object):
    def __init__(self, named_pipe):
        self.named_pipe = named_pipe

    def __send(self, stats):
        try:
            self.f = open(self.named_pipe, 'wb')
            cPickle.dump(stats, self.f, cPickle.HIGHEST_PROTOCOL)
            self.f.close()
        except IOError as xxx_todo_changeme:
            self._ex = xxx_todo_changeme
            if self._ex.errno == 32:
                #means broken pipe, happens if the StatServer runs out of data
                #and needs to re-open the connection, because of a thrown EOFError
                return False
            else:
                raise self._ex
        else:
            return True

    def send(self, stats):
        #if there is a broken pipe, try to send data again (Try five times)
        for _ in xrange(5):
            if self.__send(stats):
                return
        raise self._ex

if __name__ == "__main__":
    stats = {'stats':{'query' : "Test"}}
    stats['stats'].update({'api' : 'listPrimaryDSTypes',
                           'client_request_timing' : 1.0,
                           'server_request_timing' : 1.0,
                           'server_request_timestamp' : 123344667,
                           'request_content_length' : 102})
    
    #stats_client = StatsXMLRPCClient("localhost", 9876)
    stats_client = StatsPipeClient("/tmp/fifotest")
    stats_client.send(stats)
    #stats_client.shutdown_server()
