"""
reStructuredText docstring parser (SPHINX auto-generated documentation)
"""
from docutils.core import publish_doctree


class DocStringParser(object):
    def __init__(self):
        self._html_output = []

    def get_text(self, node_list):
        return_value = []
        for node in node_list:
            if node.nodeType == node.TEXT_NODE:
                return_value.append(node.data)
        return ''.join(return_value)

    def handle_description(self, description):
        self._html_output.append('<tr>')
        self._html_output.append('<td valign="top"><b>API description:</b></td>')
        self._html_output.append('<td valign="top">%s</td>' % self.get_text(description.childNodes))
        self._html_output.append('</tr>')

    def handle_docstring(self, docstring):
        self._html_output = ['<table>']
        ###first paragraph field is the description
        try:
            self.handle_description(docstring.getElementsByTagName('paragraph')[0])
        except IndexError:  # if docstring is an empty string
            self._html_output.append('<tr><td valign="top" colspan="2"><b>No documentation available.\
             Empty docstring!</b></td></tr>')
        else:
            self.handle_field_nodes(docstring.getElementsByTagName('field'))
        self._html_output.append('</table>')

    def handle_field_body(self, field_body):
        try:
            paragraph = field_body.getElementsByTagName('paragraph')[0]
        except IndexError:
            self._html_output.append('<td>&nbsp;</td>')
        else:
            self._html_output.append('<td valign="top">%s</td>' % self.get_text(paragraph.childNodes))

    def handle_field_name(self, field_name):
        self._html_output.append('<td valign="top" style="padding-right: 10px;"><b>%s:</b></td>'
                                 % self.get_text(field_name.childNodes))

    def handle_field_node(self, field_node):
        self.handle_field_name(field_node.getElementsByTagName('field_name')[0])
        self.handle_field_body(field_node.getElementsByTagName('field_body')[0])

    def handle_field_nodes(self, field_nodes):
        for count, field_node in enumerate(field_nodes):
            ### print separator line between groups
            if not count % 2:
                self._html_output.append('<tr><td bgcolor="#6D7B8D" colspan="2" \
                style="line-height:0.5px;">&nbsp;</td></tr>')
            self._html_output.append('<tr>')
            self.handle_field_node(field_node)
            self._html_output.append('</tr>')

    def get_html(self):
        return '\n'.join(self._html_output)


def parse_docstring(docstring):
    try:
        docstring = publish_doctree(docstring).asdom()
    except AttributeError:  # if no docstring is available __doc__ returns None
        return '<table><tr><td colspan="2"><b>No documentation available. Empty docstring!</b></td></tr></table>'
    else:
        dsp = DocStringParser()
        dsp.handle_docstring(docstring)
        return dsp.get_html()
