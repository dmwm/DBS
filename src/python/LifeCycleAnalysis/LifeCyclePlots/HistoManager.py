class HistoManager(object):
    def __init__(self):
        self._histos = []

    def add_histo(self, histo):
        if histo not in self._histos:
            self._histos.append(histo)

    def remove_histo(self, histo):
        try:
            self._histos.remove(histo)
        except ValueError:
            pass

    def update_histos(self, data):
        for histo in self._histos:
            histo.update(data)

    def draw_histos(self):
        for histo in self._histos:
            histo.draw()
