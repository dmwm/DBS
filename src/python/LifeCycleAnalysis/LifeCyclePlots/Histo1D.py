from ROOT import TCanvas, TFile, TH1F

class Histo1D(object):
    def __init__(self, name, title, nbins, xmin, xmax,
                 value_to_fill,
                 fill_fkt=lambda histo, x: x[histo._value_to_fill],
                 condition=lambda x: True,
                 color='black'):
        
        self._histogram = TH1F(name, title, nbins, xmin, xmin)
        self._name = name
        self._value_to_fill = value_to_fill
        self._fill_fkt = fill_fkt
        self._condition = condition
        self._color = color

    def update(self, data):
        if self._condition(data):
            self._histogram.Fill(self._fill_fkt(self, data))

    def draw(self):
        canvas = TCanvas(self._name, self._name)
        canvas.cd()
        self._histogram.Draw()
