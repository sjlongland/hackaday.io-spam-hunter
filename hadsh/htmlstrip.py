#!/usr/bin/env python

# HTML Stripper tool
# Credit: https://stackoverflow.com/a/7778368

try:
    # Python 3.4+
    from html.parser import HTMLParser
except ImportError:
    # Python 2.x
    from HTMLParser import HTMLParser

try:
    # Python 3.4+
    from html import entities as htmlentitydefs
except ImportError:
    # Python 2.7
    import htmlentitydefs


class HTMLTextExtractor(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.result = [ ]

    def handle_data(self, d):
        self.result.append(d)

    def handle_charref(self, number):
        codepoint = int(number[1:], 16) \
                if number[0] in (u'x', u'X') \
                else int(number)
        self.result.append(unichr(codepoint))

    def handle_entityref(self, name):
        codepoint = htmlentitydefs.name2codepoint[name]
        self.result.append(unichr(codepoint))

    def get_text(self):
        return u''.join(self.result)


def html_to_text(html):
    s = HTMLTextExtractor()
    s.feed(html)
    return s.get_text()
