#!/usr/bin/env python
from __future__ import print_function
from optparse import OptionParser, OptionGroup
from SimpleXMLRPCServer import SimpleXMLRPCServer
import cPickle
import sqlite3 as sqlite
import os, signal, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.set_defaults(port = 9876)
    parser.add_option("-x", "--xmlrpc", action="store_true", dest="xml", help="Start XMLRPC StatServer")
    parser.add_option("-n", "--npipe", action="store_true", dest="pipe", help="Start named pipe StatServer")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output DB File")
    
    xml_group = OptionGroup(parser, "Options for XMLRPC Server")
    xml_group.add_option("-p", "--port", type="string", dest="port", help="port to run the server")

    parser.add_option_group(xml_group)

    pipe_group = OptionGroup(parser, "Options for named pipe Server")
    pipe_group.add_option("-i", "--input", type="string", dest="pipe_name", help="Filename of the named pipe")

    parser.add_option_group(pipe_group)
    
    (options, args) = parser.parse_args()

    if (options.xml and options.pipe) or (options.xml==None and options.pipe==None):
        parser.print_help()
        parser.error("You need to provide one of the following options, --xmlrpc or --npipe")

    if not options.output:
        parser.print_help()
        parser.error("You need to provide following options, --out=OutputDB.db")

    if options.pipe and not options.pipe_name:
        parser.print_help()
        parser.error("You need to provide following options, --input=named_pipe")
        
    return options

class SqlStats(object):
    def __init__(self, dbfile):
        self.conn = sqlite.connect(dbfile)

        #clean-up and recreate db
        with self.conn:
            cur = self.conn.cursor()
            ### Performance Tuning
            cur.execute("PRAGMA cache_size=200000")
            cur.execute("PRAGMA synchronous = 0")
            cur.execute("DROP TABLE IF EXISTS Statistics")
            cur.execute("DROP TABLE IF EXISTS Failures")
            cur.execute("DROP TABLE IF EXISTS MigrationStatistics")
            cur.execute("CREATE TABLE Statistics(Id INTEGER PRIMARY KEY, Query TEXT, ApiCall TEXT, ClientTiming DOUBLE,\
                        ServerTiming DOUBLE, ServerTimeStamp INT, ContentLength INT)")
            cur.execute("CREATE TABLE Failures(Id INTEGER PRIMARY KEY, Query TEXT, ApiCall TEXT, Type TEXT, Value TEXT,\
                        Traceback TEXT)")
            cur.execute("CREATE TABLE MigrationStatistics(Id INTEGER PRIMARY KEY, Data TEXT, StartTime DOUBLE, \
                        EndTime DOUBLE, Status TEXT)")
   
    def add_data(self, data):
        stats = data.get("stats")
        failures = data.get("failures")
        migration_stats = data.get("migration_stats")

        with self.conn:
            cur = self.conn.cursor()

            if failures:
                values = (str(stats.get('query')),
                          str(stats.get('api')),
                          str(failures.get('type')),
                          str(failures.get('value')),
                          str(failures.get('traceback')))

                cur.execute('INSERT INTO Failures(Query, ApiCall, Type, Value, Traceback) VALUES%s' % str(values))

            #in case of failures do not insert stats
            elif migration_stats:
                values = (str(migration_stats.get('data')),
                          float(migration_stats.get('start_time')),
                          float(migration_stats.get('end_time')),
                          str(migration_stats.get('status')))

                cur.execute('INSERT INTO MigrationStatistics(Data, StartTime, EndTime, Status) VALUES%s' % str(values))

            else:
                values = (str(stats.get('query')),
                          str(stats.get('api')),
                          float(stats.get("client_request_timing")),
                          float(stats.get("server_request_timing")),
                          float(stats.get("server_request_timestamp")),
                          int(stats.get("request_content_length")))

                cur.execute('INSERT INTO Statistics(Query, ApiCall, ClientTiming, ServerTiming, ServerTimeStamp,\
                            ContentLength) VALUES%s' % str(values))

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
        print("Caught signal", signum)
        self.shutdown()

    def shutdown(self):
        self.exit_server=True
        return 1

    def serve_forever(self):
        while not self.exit_server: self.handle_request()

class StatsPipeServer(object):
    exit_server = False

    def __init__(self, pipe_name):
        self.func = None
        self.pipe_name = pipe_name
        os.mkfifo(pipe_name)
        
    def handle_request(self):
        while True:
            try:
                input_data = cPickle.load(self.f)
            except EOFError: ### no new data available
                return
            else:
                self.func(input_data)

    def register_function(self, func):
        self.func = func

    def register_signal(self, signum):
        signal.signal(signum, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("Caught signal", signum)
        self.shutdown()

    def shutdown(self):
        self.exit_server=True
        try:
            self.f.close()
        except AttributeError: ### file object not yet created
            pass
        finally:
            os.unlink(self.pipe_name)

    def serve_forever(self):
        while not self.exit_server:
            ### named pipes are blocking code is waiting until something is written to the pipe
            self.f = open(self.pipe_name, 'rb')
            ### handle all queued requests, afterwards EOFError is thrown
            self.handle_request()
            ### After EOFError the file needs to be closed and re-opened
            self.f.close()

if __name__ == "__main__":
    options = get_command_line_options(os.path.basename(__file__), sys.argv)
    sql_stats = SqlStats(dbfile=options.output)

    if options.xml:
        stats_server = StatsXMLRPCServer(("localhost", options.port))
        stats_server.register_function(sql_stats.add_data)
        stats_server.register_function(stats_server.shutdown)
        stats_server.register_signal(signal.SIGHUP)
        stats_server.register_signal(signal.SIGINT)

    if options.pipe:
        stats_server = StatsPipeServer(pipe_name=options.pipe_name)
        stats_server.register_function(sql_stats.add_data)
        stats_server.register_signal(signal.SIGHUP)
        stats_server.register_signal(signal.SIGINT)

    stats_server.serve_forever()
