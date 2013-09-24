#!/usr/bin/env python2.6
from ROOT import gROOT, TFile
from LifeCycleAnalysis.LifeCyclePlots.LifeCyclePlotManager import LifeCyclePlotManager
from LifeCycleAnalysis.LifeCyclePlots.SqliteDAO import SqliteDAO
from LifeCycleAnalysis.LifeCyclePlots.SqliteDAO import sqlite
from LifeCycleAnalysis.LifeCyclePlots.WebView import WebView

from optparse import OptionParser
import os, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input DB File")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output Root File")
    parser.add_option("-p", "--print", type="string", dest="print_format", help="Print histograms in format")
    parser.add_option("-d", "--description", type="string", dest="description", help="Description for current measurement (name of output directory)")
    parser.add_option("-w", "--web-site", action="store_true", dest="website", help="Create a web site from results", default=False)
    parser.add_option("-b", "--batch", action="store_true", dest="batch", help="Run LifeCyclePlots in batch mode", default=False)

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.sql (mandatory), --out=plot.root (optional)\n
        --print <format> (optional), but --description needed in that case (name of output directory)\n
        --web-site (optional), but --print <format> and --description needed"""
    
    if not options.input:
        parser.print_help()
        parser.error(error_msg)

    if (options.print_format or options.website) and not (options.print_format and options.description):
        parser.print_help()
        parser.error(error_msg)

    return options

if __name__ == "__main__":
    options = get_command_line_options(os.path.basename(__file__), sys.argv)

    ### create output directory
    if options.description and options.print_format:
        try:
            os.mkdir(options.description)
        except OSError as oserr:
            print "Directory %s does already exists, please clean-up." % (options.description)
            raise oserr

    gROOT.Reset()

    if options.batch:
        gROOT.SetBatch(True)

    sqlite_dao = SqliteDAO(options.input)

    ### fetch all APIs called during the test
    list_of_apis = sqlite_dao.get_unique_column_list('Statistics', 'ApiCall')

    ### fetch begin and end of the test
    starttime, endtime = sqlite_dao.get_column_min_max('Statistics', 'ServerTimeStamp')

    ### get list of errors occurred
    list_of_errors = sqlite_dao.get_unique_column_list('Failures', 'Value')

    ### plot reader or/and writer tests
    reader_tests = filter(lambda x: x.startswith('list') or x.startswith('status'), list_of_apis)
    writer_tests = filter(lambda x: x.startswith('insert') or x.startswith('update') or x.startswith('submit'),
                          list_of_apis)
    migration_tests = sqlite_dao.table_exists(table='MigrationStatistics')

    statistic_categories = list()

    if reader_tests:
        statistic_categories.append('reader_stats')
    if writer_tests:
        statistic_categories.append('writer_stats')
    if migration_tests:
        statistic_categories.append('migration_stats')

    categories = statistic_categories + ['failures']

    ### create all plots
    plot_manager = LifeCyclePlotManager(categories=categories,
                                        list_of_apis=list_of_apis,
                                        list_of_errors=list_of_errors,
                                        starttime=starttime,
                                        endtime=endtime)

    for row in sqlite_dao.get_rows('Statistics'):
        if row['ApiCall'] in reader_tests:
            plot_manager.update_histos(row, category='reader_stats')
        else:
            plot_manager.update_histos(row, category='writer_stats')

    for row in sqlite_dao.get_rows('Failures'):
        plot_manager.update_histos(row, category='failures')

    try:#old data does not contain that table
        for row in sqlite_dao.get_rows('MigrationStatistics'):
            plot_manager.update_histos(row, category='migration_stats')
    except sqlite.OperationalError as ex:
        if "no such table" not in ex:
            raise ex

    if reader_tests and writer_tests:
        plot_manager.add_stacked_histos(categories=['reader_stats','writer_stats'])

    plot_manager.draw_histos()

    if options.description and options.print_format:
        plot_manager.save_histos_as(output_directory=options.description, format=options.print_format)

        if options.website:
            web_view = WebView(options.description, plot_manager.histo_names, options.print_format)
            web_view.create_web_view('index.html')
