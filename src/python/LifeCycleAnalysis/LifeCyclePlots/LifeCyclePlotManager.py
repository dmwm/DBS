from LifeCycleAnalysis.LifeCyclePlots.HistoManager import HistoManager
from LifeCycleAnalysis.LifeCyclePlots.Histogram import Histo1D, Histo2D

from itertools import chain
import inspect

class StatisticPlots(object):
    _plots_to_generate = ['_create_client_request_timing',
                          '_create_server_request_timing',
                          '_create_client_request_timing_hires',
                          '_create_server_request_timing_hires',
                          '_create_content_length',
                          '_create_access_per_second',
                          '_create_client_request_timing_vs_content_length',
                          '_create_server_request_timing_vs_content_length',
                          '_create_client_request_timing_vs_server_request_timing',
                          '_create_client_request_timing_vs_api',
                          '_create_client_server_timing_vs_api',
                          '_create_api_access_counter']

    def __init__(self, list_of_apis, starttime, endtime, color):
        self._list_of_apis = list_of_apis
        # api as keys and numbers as value, to fill 0,1,2,3,4 bins in APIAccessCounter histogramm and
        # to set bin label later accordingly
        self._enumerated_dict_of_apis = dict(zip(self._list_of_apis, xrange(len(self._list_of_apis))))
        self._starttime = starttime
        self._endtime = endtime
        self._color = color
        self._marker = 2 #dots
        self._plots = list()

    def __iter__(self):
        return iter(self._plots)

    def create_plots(self):
        ###create generic plots first, to get the right order
        for plot in self._plots_to_generate:
            create_plot = getattr(self, plot)
            self._plots.extend(create_plot())

        ###create api specific plots
        for plot in self._plots_to_generate:
            create_plot = getattr(self, plot)
            arg_spec = inspect.getargspec(create_plot)
            if 'api' in arg_spec.args:
                for api in self._list_of_apis:
                    self._plots.extend(create_plot(api))

    def _create_client_request_timing(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='ClientRequestTiming%s' %api, title='Client Request Timing (%s)' % api,
                        xnbins=1000, xmin=0., xmax=10.,
                        condition=condition,
                        x_value_to_fill="ClientTiming",
                        label={'x':"Time [s]",'y':"#"},
                        color={'line':self._color,'fill':self._color},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10.0)})]

    def _create_server_request_timing(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='ServerRequestTiming%s' % api, title='Server Request Timing (%s)' % api,
                        xnbins=1000, xmin=0., xmax=10.,
                        condition=condition,
                        x_value_to_fill="ServerTiming",
                        label={'x':"Time [s]",'y':"#"},
                        color={'line':self._color, 'fill':self._color},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10.0)})]

    def _create_client_request_timing_hires(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='ClientRequestTimingHighRes%s' % api, title='Client Request Timing (%s)' % api,
                        xnbins=100, xmin=0., xmax=0.5,
                        condition=condition,
                        x_value_to_fill="ClientTiming",
                        label={'x':"Time [s]",'y':"#"},
                        color={'line':self._color,'fill':self._color},
                        add_options={'GetXaxis.SetRangeUser':(0.0,0.5)})]

    def _create_server_request_timing_hires(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='ServerRequestTimingHighRes%s' % api, title='Server Request Timing (%s)' % api,
                        xnbins=100, xmin=0., xmax=0.5,
                        condition=condition,
                        x_value_to_fill="ServerTiming",
                        label={'x':"Time [s]",'y':"#"},
                        color={'line':self._color, 'fill':self._color},
                        add_options={'GetXaxis.SetRangeUser':(0.0,0.5)})]

    def _create_content_length(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='ContentLength%s' % api, title='Content Length (%s)' % api,
                        xnbins=100, xmin=0, xmax=10000,
                        condition=condition,
                        x_value_to_fill="ContentLength",
                        label={'x':"Size [bytes]",'y':"#"},
                        color={'line':self._color,'fill':self._color},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10000.0)})]

    def _create_access_per_second(self, api='All'):
        if api=='All':
            condition = lambda x: True
        else:
            condition = lambda x, local_api=api: (x['ApiCall']==local_api)

        return [Histo1D(name='AccessesPerSecond%s' % api, title='Accesses per Second (%s)' % api,
                        xnbins=int(self._endtime-self._starttime), xmin=0, xmax=self._endtime-self._starttime,
                        condition=condition,
                        x_value_to_fill="ServerTimeStamp",
                        fill_fkt=lambda histo, x: (x[histo._x_value_to_fill]-self._starttime, 1),
                        label={'x':"time [s]",'y':"#"},
                        color={'line':self._color})]

    def _create_client_request_timing_vs_content_length(self):
        return [Histo2D(name='ClientRequestTimingVsContentLength', title='Client Request Timing Vs Content Length',
                        xnbins=1000, xmin=0., xmax=10.,
                        ynbins=100, ymin=0., ymax=10000.,
                        x_value_to_fill="ClientTiming",
                        y_value_to_fill="ContentLength",
                        label={'x':"Time [s]",'y':"Content Length [bytes]"},
                        color={'line':self._color,'marker':self._marker},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10.0)})]

    def _create_server_request_timing_vs_content_length(self):
        return [Histo2D(name='ServerRequestTimingVsContentLength', title='Server Request Timing Vs Content Length',
                        xnbins=1000, xmin=0., xmax=10.,
                        ynbins=100, ymin=0., ymax=10000.,
                        x_value_to_fill="ServerTiming",
                        y_value_to_fill="ContentLength",
                        label={'x':"Time [s]",'y':"Content Length [bytes]"},
                        color={'line':self._color, 'marker':self._marker},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10.0)})]

    def _create_client_request_timing_vs_server_request_timing(self):
        return [Histo2D(name='ClientRequestTimingVsServerRequestTiming', title='Client Request Timing Vs Server Request Timing',
                        xnbins=1000, xmin=0., xmax=10.,
                        ynbins=1000, ymin=0., ymax=10.,
                        x_value_to_fill="ClientTiming",
                        y_value_to_fill="ServerTiming",
                        label={'x':"Client Time [s]",'y':"Server Time [s]"},
                        color={'line':self._color,'marker':self._marker},
                        add_options={'GetXaxis.SetRangeUser':(0.0,10.0)})]

    def _create_client_request_timing_vs_api(self):
        histo = Histo2D(name='ClientRequestTimingVsAPI', title='Client Request Timing Vs API',
                        xnbins=len(self._list_of_apis), xmin=0., xmax=len(self._list_of_apis),
                        ynbins=1000, ymin=0., ymax=10.,
                        fill_fkt=lambda histo, x, bla=self._enumerated_dict_of_apis: (bla.get(x[histo._x_value_to_fill])+0.0001, x[histo._y_value_to_fill], 1),
                        x_value_to_fill="ApiCall",
                        y_value_to_fill="ClientTiming",
                        label={'y':"Client Time [s]"},
                        color={'line':self._color,'marker':self._marker})

        for api in self._list_of_apis:
            histo.histogram.GetXaxis().SetBinLabel(self._enumerated_dict_of_apis.get(api)+1,api) # Bin enumerations starts at 1

        return [histo]

    def _create_client_server_timing_vs_api(self):
        histo = Histo2D(name='Client-ServerTimingVsAPI', title='Client Timing - Server Timing Vs API',
                        xnbins=len(self._list_of_apis), xmin=0., xmax=len(self._list_of_apis),
                        ynbins=1000, ymin=0., ymax=10.,
                        fill_fkt=lambda histo, x, bla=self._enumerated_dict_of_apis: (bla.get(x[histo._x_value_to_fill])+0.0001, x[histo._y_value_to_fill]-x['ServerTiming'], 1),
                        x_value_to_fill="ApiCall",
                        y_value_to_fill="ClientTiming",
                        label={'y':"Client Time [s]"},
                        color={'line':self._color,'marker':self._marker},
                        add_options={'GetYaxis.SetRangeUser':(0.0,2.0)})

        for api in self._list_of_apis:
            histo.histogram.GetXaxis().SetBinLabel(self._enumerated_dict_of_apis.get(api)+1,api) # Bin enumerations starts at 1

        return [histo]

    def _create_api_access_counter(self):
        return [Histo1D(name='APIAccessCounter', title='Count of API Accesses',
                        xnbins=len(self._list_of_apis), xmin=0, xmax=len(self._list_of_apis)+1,
                        fill_fkt=lambda histo, x: (x[histo._x_value_to_fill], 1),
                        x_value_to_fill="ApiCall",
                        log={'y':True},
                        color={'fill':2},
                        stats=False,
                        draw_options="bar0",
                        add_options={'SetBarWidth':(0.9,),
                                     'SetBarOffset':(0.05,),
                                     'GetXaxis.SetLabelSize': (0.042,)})]

class FailurePlots(object):
    _plots_to_generate = ['_create_exceptions_plot']

    def __init__(self, list_of_errors, color):
        self._list_of_errors = list_of_errors
        self._color = color
        self._plots = list()

    def __iter__(self):
        return iter(self._plots)

    def create_plots(self):
        for plot in self._plots_to_generate:
            self._plots.extend(getattr(self, plot)())

    def _create_exceptions_plot(self):
        return [Histo1D(name="Exceptions", title='Exceptions',
                        xnbins=len(self._list_of_errors), xmin=0, xmax=len(self._list_of_errors)+1,
                        fill_fkt=lambda histo, x: (x[histo._x_value_to_fill].split(':')[0], 1) if x[histo._x_value_to_fill].find('HTTP Error')!=-1 else (x[histo._x_value_to_fill], 1),
                        x_value_to_fill="Value",
                        color={'fill':self._color},
                        stats=False,
                        draw_options="bar0",
                        add_options={'SetBarWidth':(0.9,),
                                     'SetBarOffset':(0.05,),
                                     'GetXaxis.SetLabelSize': (0.042,),
                                     'SetMinimum':(0.0,)})]

class LifeCyclePlotManager(object):
    _supported_categories = ['reader_stats', 'writer_stats', 'failures']

    def __init__(self, categories, list_of_apis, list_of_errors, starttime, endtime):
        plot_creator = {'reader_stats' : StatisticPlots(filter(lambda x: x.startswith('list'), list_of_apis),
                                                        starttime=starttime,
                                                        endtime=endtime,
                                                        color=2),
                        'writer_stats' : StatisticPlots(filter(lambda x: x.startswith('insert') or x.startswith('update'), list_of_apis),
                                                        starttime=starttime,
                                                        endtime=endtime,
                                                        color=2),
                        'failures' : FailurePlots(list_of_errors,
                                                  color=2)}

        self._histo_managers = dict()
        for category in categories:
            if category not in self._supported_categories:
                raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
            self._histo_managers[category]= HistoManager()
            plot_creator[category].create_plots()
            self.add_histos(plot_creator[category], category)

    @property
    def histo_names(self):
        histo_names = list()
        for histo_manager in self._histo_managers.itervalues():
            histo_names.extend(histo_manager.histo_names)
        return histo_names

    def add_histo(self, histo, category):
        if category not in self._supported_categories:
            raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
        self._histo_managers[category].add_histo(histo)

    def add_histos(self, histos, category):
        if category not in self._supported_categories:
            raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
        for histo in histos:
            self._histo_managers[category].add_histo(histo)

    def remove_histo(self, histo, category):
        if category not in self._supported_categories:
            raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
        self._histo_managers[category].remove_histo(histo)

    def update_histos(self, data, category):
        if category not in self._supported_categories:
            raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
        self._histo_managers[category].update_histos(data)

    def draw_histos(self):
        for histo_manager in self._histo_managers.itervalues():
            histo_manager.draw_histos()

    def save_histos_as(self, output_directory, format="png"):
        for histo_manager in self._histo_managers.itervalues():
            histo_manager.save_histos_as(output_directory, format)

    def add_stacked_histos(self, categories=['reader_stats', 'writer_stats']):
        #find overlap
        unique_histo_names = None
        for category in categories:
            if category not in self._supported_categories:
                raise NameError('Category %s is not supported by %s' % (category, self.__class__.__name__))
            if unique_histo_names:
                unique_histo_names.intersection(self._histo_managers[category].histo_names)
            else:
                unique_histo_names= set(self._histo_managers[category].histo_names)

        self._histo_managers['stacked_histos'] = HistoManager()
