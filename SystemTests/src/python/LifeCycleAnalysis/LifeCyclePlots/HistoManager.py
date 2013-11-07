from itertools import chain

class HistoManager(object):
    def __init__(self, histos=[]):
        ###need ordering, therefore dict isn't used here
        self._histos = list(histos)
        self._histos_names = list()
        for histo in histos:
            self._histos_names = histo.name

    def __add__(self, other):
        return HistoManager(chain(self, other))

    def __iter__(self):
        return iter(self._histos)

    @property
    def histo_names(self):
        return self._histos_names

    def add_histo(self, histo):
        if histo.name in self._histos_names:
            raise NameError('Name %s already exists. Names must be unique.' % histo.name)
        self._histos.append(histo)
        self._histos_names.append(histo.name)

    def remove_histo(self, histo):
        try:
            index = self._histos_names.index(histo.name)
            self._histos_names.pop(index)
            self._histos.pop(index)
        except IndexError:
            raise IndexError('No histogram with name %s exists' % histo.name)

    def update_histos(self, data):
        for histo in self:
            histo.update(data)

    def draw_histos(self):
        for histo in self:
            histo.draw()

    def save_histos_as(self, output_directory, format="png"):
        for histo in self:
            histo.save_as(output_directory, format)
