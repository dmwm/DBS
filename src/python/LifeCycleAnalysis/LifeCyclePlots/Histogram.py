from ROOT import gStyle, TCanvas, TFile, TH1F, TH2F, TPaveStats

class BasicHisto(object):
    def __init__(self, name, title, fill_fkt, condition, label,
                 log, color, stats, draw_options, add_options):
        self._name = name
        self._title = title
        self._fill_fkt = fill_fkt
        self._condition = condition
        self._label = label
        self._log = log
        self._color = color
        self._stats = stats
        self._draw_options = draw_options
        self._add_options = add_options

    @property
    def histogram(self):
        return self._histogram

    @property
    def name(self):
        return self._name

    @property
    def title(self):
        return self._title

    def update(self, data):
        if self._condition(data):
            self._histogram.Fill(*self._fill_fkt(self, data))

    def create_canvas(self):
        self._canvas = TCanvas(self._name, self._name)

        if self._stats:
            self._canvas.SetRightMargin(0.183908)
            self._canvas.SetTopMargin(0.06991526)
            gStyle.SetOptStat("mreou");

        gStyle.SetCanvasBorderMode(0);
        gStyle.SetPadBorderMode(0);

        self._canvas.cd()

    def draw(self):
        # create canvas to draw histogram
        if not getattr(self,'_canvas', None):
            self.create_canvas()

        # set axis label
        self._histogram.GetXaxis().SetTitle(self._label.get('x',""));
        self._histogram.GetYaxis().SetTitle(self._label.get('y',""));
        # set colors
        self._histogram.SetFillColor(self._color.get("fill",0))
        self._histogram.SetLineColor(self._color.get("line",1))
        # set log axis
        self._canvas.SetLogx(self._log.get('x',False))
        self._canvas.SetLogy(self._log.get('y',False))

        # set additional options
        for key, value in self._add_options.iteritems():
            # initialize callable with histogramm to start for-loop
            # to resolve nested calls like GetXaxis().SetLabelSize()
            callable_fkt = self._histogram

            for func in key.split('.'):
                if callable(callable_fkt):# to address nested function calls in ROOT
                    callable_fkt = getattr(callable_fkt(), func)
                else: #not nested function call directly to histogram itself
                    callable_fkt = getattr(callable_fkt, func)

            callable_fkt(*value)

        #set my stat style
        if self._stats:
            self.set_stats_style()
        else:
            self._histogram.SetStats(False)

        #draw histogram to canvas
        self._histogram.Draw(self._draw_options)

    def save_as(self, output_directory, format="png"):
        try:
            self._canvas.Print("%s/%s.%s" % (output_directory, self._name, format), format)
        except AttributeError:
            print "You have to draw histograms before saving."
            pass

    def set_stats_style(self):
        stat_box = TPaveStats(0.816092,0.7415254,0.9971264,0.9300847,"brNDC")
        stat_box.SetBorderSize(1);
        stat_box.SetFillColor(0);
        stat_box.SetTextAlign(12);
        stat_box.SetTextFont(42);
        stat_box.SetName("stats_%s" % (self._name))
        stat_box.Draw()
        self._histogram.GetListOfFunctions().Add(stat_box)
        stat_box.SetParent(self._histogram.GetListOfFunctions())
        self._canvas.Modified()
        self._canvas.Update()

class Histo1D(BasicHisto):
    def __init__(self, name, title, xnbins, xmin, xmax,
                 x_value_to_fill,
                 fill_fkt=lambda histo, x: (x[histo._x_value_to_fill], 1),# Fill function needs value and weigth
                 condition=lambda x: True,
                 label={'x' : "", 'y' : ""},
                 log={'x' : False, 'y' : False},
                 color={'line':1, 'fill':0}, #ROOT Color Codes
                 stats=True,
                 draw_options="",
                 add_options={}):
        super(Histo1D, self).__init__(name, title, fill_fkt, condition, label, log, color, stats, draw_options, add_options)
        self._histogram = TH1F(name, title, xnbins, xmin, xmax)
        self._x_value_to_fill = x_value_to_fill


class Histo2D(BasicHisto):
    def __init__(self, name, title, xnbins, xmin, xmax, ynbins, ymin, ymax,
                 x_value_to_fill,
                 y_value_to_fill,
                 fill_fkt=lambda histo, x: (x[histo._x_value_to_fill], x[histo._y_value_to_fill], 1),# Fill function needs values and weigth
                 condition=lambda x: True,
                 label={'x' : "", 'y' : ""},
                 log={'x' : False, 'y' : False},
                 color={'line':1, 'fill':0}, #ROOT Color Codes
                 stats=True,
                 draw_options="",
                 add_options={}):
        super(Histo2D, self).__init__(name, title, fill_fkt, condition, label, log, color, stats, draw_options, add_options)
        self._histogram = TH2F(name, title, xnbins, xmin, xmax, ynbins, ymin, ymax)
        self._x_value_to_fill = x_value_to_fill
        self._y_value_to_fill = y_value_to_fill
