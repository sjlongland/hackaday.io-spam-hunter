#!/usr/bin/env python

from .htmlstrip import html_to_text
from polyglot.text import Text


def tokenise(html_text):
    """
    Return a list of words that appear in the text.
    """
    return list(Text(html_to_text(html_text)).lower().words)


def frequency(wordlist, freq=None):
    """
    Scan the word list given and count how often each word appears.
    """
    if freq is None:
        freq = {}
    for w in wordlist:
        try:
            freq[w] += 1
        except KeyError:
            freq[w] = 1
    return freq


def adjacency(wordlist, freq=None):
    """
    Scan the word list and count how often each pair of words appears.
    """
    if freq is None:
        freq = {}
    for prev_w, next_w in zip(wordlist[:-1], wordlist[1:]):
        try:
            freq[(prev_w, next_w)] += 1
        except KeyError:
            freq[(prev_w, next_w)] = 1
    return freq
