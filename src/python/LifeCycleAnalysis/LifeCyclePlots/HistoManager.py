class HistoManager(object):
    def __init__(self):
        self._histos = []

    def __iter__(self):
        for histo in self._histos:
            yield histo
        return

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

    def save_histos_as(self, output_directory, format="png"):
        for histo in self._histos:
            histo.save_as(output_directory, format)
