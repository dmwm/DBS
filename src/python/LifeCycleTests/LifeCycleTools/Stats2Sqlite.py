from optparse import OptionParser
import sqlite3 as sqlite
import glob, json, os, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-d", "--directory", type="string", dest="directory", help="start directory")
    parser.add_option("-f", "--filter", type="string", dest="filter", help="Stat file filter")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output DB File")

    (options, args) = parser.parse_args()
    
    if not (options.directory and options.filter and options.output):
        parser.print_help()
        parser.error("You need to provide following options, --directory=StartDirectory, --filter=*.stat and --out=OutputDB.db")

    return options

def get_stat_files(directory, filter):
    return glob.glob(os.path.join(directory, filter))

def insertStats(cursor, stats):
    values = (str(stats.get('query')),
              str(stats.get('api')),
              float(stats.get("client_request_timimg")),
              float(stats.get("server_request_timing")),
              int(stats.get("server_request_timestamp")),
              int(stats.get("request_content_length")))
    #print values
    cur.execute('INSERT INTO Statistics(Query, ApiCall, ClientTiming, ServerTiming, ServerTimeStamp, ContentLength) VALUES%s' % str(values))

if __name__ == '__main__':
    options = get_command_line_options(os.path.basename(__file__), sys.argv)

    file_names = get_stat_files(options.directory, options.filter)

    conn = sqlite.connect(options.output)

    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Statistics")
        cur.execute("CREATE TABLE Statistics(Id INTEGER PRIMARY KEY, Query TEXT, ApiCall TEXT, ClientTiming DOUBLE, ServerTiming DOUBLE, ServerTimeStamp INT, ContentLength INT)")
        
        for file_name in file_names:
            with file(file_name,'r') as f:
                try:
                    stats = json.load(f).get("stats")
                except ValueError as ex:
                    print "Open file %s" % (file_name)
                    print ex
                    pass
                
            insertStats(cur, stats)
