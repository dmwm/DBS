#!/usr/bin/env python2.6
from ROOT import gROOT, TFile, TH1
from LifeCycleAnalysis.LifeCyclePlots.HistoManager import HistoManager
from LifeCycleAnalysis.LifeCyclePlots.Histogram import Histo1D, Histo2D
from LifeCycleAnalysis.LifeCyclePlots.WebView import WebView

from optparse import OptionParser
import sqlite3 as sqlite
import os, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input DB File")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output Root File")
    parser.add_option("-p", "--print", type="string", dest="print_format", help="Print histograms in format")
    parser.add_option("-d", "--description", type="string", dest="description", help="Description for current measurement (name of output directory)")
    parser.add_option("-w", "--web-site", action="store_true", dest="website", help="Create a web site from results", default=False)

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

    gROOT.Reset()

    list_of_apis = ['listDatasets', 'listPrimaryDSTypes', 'listFiles', 'listFileParents', 'listFileLumis']

    # api as keys and numbers as value, to fill 0,1,2,3,4 bins in APIAccessCounter histogramm and
    # to set bin label later accordingly
    enumerated_dict_of_apis = dict(zip(list_of_apis, xrange(len(list_of_apis))))

    histo_manager = HistoManager()
    histo_manager.add_histo(Histo1D(name='ClientRequestTiming', title='Client Request Timing',
                                    xnbins=1000, xmin=0., xmax=10.,
                                    x_value_to_fill="ClientTiming",
                                    label={'x':"Time [s]",'y':"#"}))

    histo_manager.add_histo(Histo1D(name='ServerRequestTiming', title='Server Request Timing',
                                    xnbins=1000, xmin=0., xmax=10.,
                                    x_value_to_fill="ServerTiming",
                                    label={'x':"Time [s]",'y':"#"}))
    
    histo_manager.add_histo(Histo1D(name='ContentLength', title='Content Length',
                                    xnbins=1000, xmin=0, xmax=1000,
                                    x_value_to_fill="ContentLength",
                                    label={'x':"Size [bytes]",'y':"#"}))

    histo_manager.add_histo(Histo1D(name='AccessPerSecond', title='Access per Second',
                                    xnbins=1000, xmin=0, xmax=1000,
                                    x_value_to_fill="ServerTimeStamp",
                                    label={'x':"unixtime [s]",'y':"#"}))

    histo_manager.add_histo(Histo2D(name='ClientRequestTimingVsContentLength', title='Client Request Timing Vs Content Length',
                                    xnbins=1000, xmin=0., xmax=10.,
                                    ynbins=1000, ymin=0., ymax=10.,
                                    x_value_to_fill="ClientTiming",
                                    y_value_to_fill="ContentLength",
                                    label={'x':"Time [s]",'y':"Content Length [bytes]"}))

    histo_manager.add_histo(Histo2D(name='ServerRequestTimingVsContentLength', title='Server Request Timing Vs Content Length',
                                    xnbins=1000, xmin=0., xmax=10.,
                                    ynbins=1000, ymin=0., ymax=10.,
                                    x_value_to_fill="ServerTiming",
                                    y_value_to_fill="ContentLength",
                                    label={'x':"Time [s]",'y':"Content Length [bytes]"}))

    histo_manager.add_histo(Histo2D(name='ClientRequestTimingVsServerRequestTiming', title='Client Request Timing Vs Server Request Timing',
                                    xnbins=1000, xmin=0., xmax=10.,
                                    ynbins=1000, ymin=0., ymax=10.,
                                    x_value_to_fill="ClientTiming",
                                    y_value_to_fill="ServerTiming",
                                    label={'x':"Client Time [s]",'y':"Server Time [s]"}))

    histo = Histo2D(name='ClientRequestTimingVsAPI', title='Client Request Timing Vs API',
                    xnbins=len(list_of_apis), ymin=0., ymax=len(list_of_apis),
                    ynbins=1000, xmin=0., xmax=10.,
                    fill_fkt=lambda histo, x, bla=enumerated_dict_of_apis: (bla.get(x[histo._x_value_to_fill])-0.0001, x[histo._y_value_to_fill], 1),
                    x_value_to_fill="ApiCall",
                    y_value_to_fill="ClientTiming",
                    label={'y':"Client Time [s]"})

    for api in list_of_apis:
        histo.histogram.GetXaxis().SetBinLabel(enumerated_dict_of_apis.get(api)+1,api) # Bin enumerations starts at 1

    histo_manager.add_histo(histo)

    histo = Histo2D(name='Client-ServerTimingVsAPI', title='Client Timing - Server Timing Vs API',
                    xnbins=len(list_of_apis), ymin=0., ymax=len(list_of_apis),
                    ynbins=1000, xmin=0., xmax=10.,
                    fill_fkt=lambda histo, x, bla=enumerated_dict_of_apis: (bla.get(x[histo._x_value_to_fill])-0.0001, x[histo._y_value_to_fill]-x['ServerTiming'], 1),
                    x_value_to_fill="ApiCall",
                    y_value_to_fill="ClientTiming",
                    label={'y':"Client Time [s]"})

    for api in list_of_apis:
        histo.histogram.GetXaxis().SetBinLabel(enumerated_dict_of_apis.get(api)+1,api) # Bin enumerations starts at 1

    histo_manager.add_histo(histo)

    histo_manager.add_histo(Histo1D(name='APIAccessCounter', title='Count of API Accesses',
                                    xnbins=len(list_of_apis), xmin=0, xmax=len(list_of_apis)+1,
                                    fill_fkt=lambda histo, x: (x[histo._x_value_to_fill], 1),
                                    x_value_to_fill="ApiCall",
                                    log={'y':True},
                                    color={'fill':2},
                                    draw_options="bar0",
                                    add_options={'SetBarWidth':(0.9,),
                                                 'SetBarOffset':(0.05,),
                                                 'GetXaxis.SetLabelSize': (0.042,)}))

    for api in list_of_apis:
        histo_manager.add_histo(Histo1D(name='ClientRequestTiming%s' % api, title='Client Request Timing (%s)' % api,
                                        xnbins=1000, xmin=0., xmax=10.,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        x_value_to_fill="ClientTiming",
                                        label={'x':"Time [s]",'y':"#"}))
        
        histo_manager.add_histo(Histo1D(name='ServerRequestTiming%s' % api, title='Server Request Timing (%s)' % api,
                                        xnbins=1000, xmin=0., xmax=10.,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        x_value_to_fill="ServerTiming",
                                        label={'x':"Time [s]",'y':"#"}))

        histo_manager.add_histo(Histo1D(name='ContentLength%s' % api, title='Content Length (%s)' % api,
                                        xnbins=1000, xmin=0, xmax=1000,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        x_value_to_fill="ContentLength",
                                        label={'x':"Size [bytes]",'y':"#"}))

        histo_manager.add_histo(Histo1D(name='AccessPerSecond%s' % api, title='Access per Second (%s)' % api,
                                        xnbins=1000, xmin=0, xmax=1000,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        x_value_to_fill="ServerTimeStamp",
                                        label={'x':"unixtime [s]",'y':"#"}))

    conn = sqlite.connect(options.input)

    with conn:
        conn.row_factory = sqlite.Row
        cur = conn.cursor()

        cur.execute('SELECT * FROM Statistics')
        rows = cur.fetchall()

    for row in rows:
        histo_manager.update_histos(row)

    histo_manager.draw_histos()
    
    if options.description and options.print_format:
        os.mkdir(options.description)
        histo_manager.save_histos_as(output_directory=options.description, format=options.print_format)

        if options.website:
            web_view = WebView(options.description, histo_manager, options.print_format)
            web_view.create_web_view('index.html')
