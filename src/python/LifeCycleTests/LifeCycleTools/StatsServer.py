#!/usr/bin/env python
from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer
import sqlite3 as sqlite
import json, os, signal, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.set_defaults(port = 9876)
    parser.add_option("-p", "--port", type="string", dest="port", help="port to run the server")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output DB File")

    (options, args) = parser.parse_args()
    
    if not options.output:
        parser.print_help()
        parser.error("You need to provide following options, --port=1234 (optional), --out=OutputDB.db")

    return options

class SqlStats(object):
    def __init__(self, dbfile):
        self.conn = sqlite.connect(dbfile)

        #clean-up and recreate db
        with self.conn:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS Statistics")
            cur.execute("CREATE TABLE Statistics(Id INTEGER PRIMARY KEY, Query TEXT, ApiCall TEXT, ClientTiming DOUBLE, ServerTiming DOUBLE, ServerTimeStamp INT, ContentLength INT)")
   
    def add_stats(self, stats):
        stats = stats.get("stats")
        
        values = (str(stats.get('query')),
                  str(stats.get('api')),
                  float(stats.get("client_request_timing")),
                  float(stats.get("server_request_timing")),
                  float(stats.get("server_request_timestamp")),
                  int(stats.get("request_content_length")))

        with self.conn:
            cur = self.conn.cursor()
            cur.execute('INSERT INTO Statistics(Query, ApiCall, ClientTiming, ServerTiming, ServerTimeStamp, ContentLength) VALUES%s' % str(values))
        return 1

class StatsXMLRPCServer(SimpleXMLRPCServer):
#modified code from http://code.activestate.com/recipes/114579-remotely-exit-a-xmlrpc-server-cleanly
    exit_server = False

    def __init__(self, *args, **kwargs):
        #cannot use super, since SimpleXMLRPCServer is a class object
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)

    def register_signal(self, signum):
        signal.signal(signum, self.signal_handler)

    def signal_handler(self, signum, frame):
        print "Caught signal", signum
        self.shutdown()

    def shutdown(self):
        self.exit_server=True
        return 1

    def serve_forever(self):
        while not self.exit_server: self.handle_request()
    
if __name__ == "__main__":
    options = get_command_line_options(os.path.basename(__file__), sys.argv)
    sql_stats = SqlStats(dbfile=options.output)
    stats_server = StatsXMLRPCServer(("localhost", options.port))
    stats_server.register_function(sql_stats.add_stats)
    stats_server.register_function(stats_server.shutdown)
    stats_server.register_signal(signal.SIGHUP)
    stats_server.register_signal(signal.SIGINT)

    stats_server.serve_forever()
