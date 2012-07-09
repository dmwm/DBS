#!/usr/bin/env python2.6
from ROOT import gROOT, TFile
from LifeCycleAnalysis.LifeCyclePlots.HistoManager import HistoManager
from LifeCycleAnalysis.LifeCyclePlots.Histo1D import Histo1D

from optparse import OptionParser
import sqlite3 as sqlite
import os, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input DB File")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output Root File")

    (options, args) = parser.parse_args()
    
    if not (options.input and options.output):
        parser.print_help()
        parser.error("You need to provide following options, --in=input.sql, --out=plot.root")

    return options

if __name__ == "__main__":
    options = get_command_line_options(os.path.basename(__file__), sys.argv)

    gROOT.Reset()

    histo_manager = HistoManager()
    histo_manager.add_histo(Histo1D(name='ClientRequestTiming', title='Client Request Timing',
                                    nbins=1000, xmin=0., xmax=10.,
                                    value_to_fill="ClientTiming"))

    histo_manager.add_histo(Histo1D(name='ServerRequestTiming', title='Server Request Timing',
                                    nbins=1000, xmin=0., xmax=10.,
                                    value_to_fill="ServerTiming"))
    
    histo_manager.add_histo(Histo1D(name='ContentLength', title='Content Length',
                                     nbins=1000, xmin=0, xmax=1000,
                                     value_to_fill="ContentLength"))

    histo_manager.add_histo(Histo1D(name='AccessPerSecond', title='Access per Second',
                                     nbins=1000, xmin=0, xmax=1000,
                                     value_to_fill="ServerTimeStamp"))

    for api in ['listDatasets', 'listPrimaryDSTypes', 'listFiles', 'listFileParents', 'listFileLumis']:
        histo_manager.add_histo(Histo1D(name='ClientRequestTiming%s' % api, title='Client Request Timing (%s)' % api,
                                        nbins=1000, xmin=0., xmax=10.,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        value_to_fill="ClientTiming"))
        
        histo_manager.add_histo(Histo1D(name='ServerRequestTiming%s' % api, title='Server Request Timing (%s)' % api,
                                        nbins=1000, xmin=0., xmax=10.,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        value_to_fill="ServerTiming"))

        histo_manager.add_histo(Histo1D(name='ContentLength%s' % api, title='Content Length (%s)' % api,
                                        nbins=1000, xmin=0, xmax=1000,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        value_to_fill="ContentLength"))

        histo_manager.add_histo(Histo1D(name='AccessPerSecond%s' % api, title='Access per Second (%s)' % api,
                                        nbins=1000, xmin=0, xmax=1000,
                                        condition=lambda x, local_api=api: (x['ApiCall']==local_api),
                                        value_to_fill="ServerTimeStamp"))
        
    ##histo_manager.add_histo(Histo1D(name='ClientRequestTimingNorm', title='Client Request Timing (norm.)',
    ##                                value_to_fill="ClientTiming"))
   
    
    conn = sqlite.connect(options.input)

    with conn:
        conn.row_factory = sqlite.Row
        cur = conn.cursor()

        cur.execute('SELECT * FROM Statistics')
        rows = cur.fetchall()

    for row in rows:
        histo_manager.update_histos(row)

    histo_manager.draw_histos()
