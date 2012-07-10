from ROOT import TCanvas, TFile, TH1F

class Histo1D(object):
    def __init__(self, name, title, nbins, xmin, xmax,
                 value_to_fill,
                 fill_fkt=lambda histo, x: x[histo._value_to_fill],
                 condition=lambda x: True,
                 x_label="",
                 y_label="",
                 log_x=False,
                 log_y=False,
                 line_color=1, #ROOT Color Codes
                 fill_color=0):
        
        self._histogram = TH1F(name, title, nbins, xmin, xmin)
        self._name = name
        self._value_to_fill = value_to_fill
        self._fill_fkt = fill_fkt
        self._condition = condition
        self._x_label = x_label
        self._y_label = y_label
        self._log_x = log_x
        self._log_y = log_y
        self._line_color = line_color
        self._fill_color = fill_color

    def update(self, data):
        if self._condition(data):
            self._histogram.Fill(self._fill_fkt(self, data))

    def draw(self):
        # create canvas to draw histogram
        if not getattr(self,'_canvas', None):
            self._canvas = TCanvas(self._name, self._name)

        self._canvas.cd()
        # set axis label
        self._histogram.GetXaxis().SetTitle(self._x_label);
        self._histogram.GetYaxis().SetTitle(self._y_label);
        # set colors
        self._histogram.SetFillColor(self._fill_color)
        self._histogram.SetLineColor(self._line_color)
        # set log axis
        self._canvas.SetLogx(self._log_x)
        self._canvas.SetLogy(self._log_y)
        #draw histogram to canvas
        self._histogram.Draw()

    def save_as(self, format="png"):
        try:
            self._canvas.Print("%s.%s" % (self._name, format), format)
        except AttributeError:
            print "You have to draw histograms before saving."
            pass
