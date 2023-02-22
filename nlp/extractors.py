# -*- coding: utf-8 -*-

"""
This module contains functions and classes for extracting
place names (and, more generally, named entitites) from text.
"""

import tenacity
from polyglot.text import Text
from polyglot.detect import Detector

from genery.utils import objectify, RecordDict, TextCleaner

from nlp.models import Model, process_absent_corpus_polyglot


def detect_lang(text, default=None):
    """
    Sniffs the language and returns its code
    (or None, if unsuccessfull).
    """
    def _do_detect(txt):
        """This helps avoiding big texts."""
        txt = TextCleaner(txt).cleanup_hard()
        try:
            detector = Detector(txt)
        except Exception:
            return default
        return detector.language.code

    lim = 1000
    if len(text) <= lim:
        return _do_detect(text)

    curr = 100
    lang = None
    while (not lang) or (curr <= lim):
        lang = _do_detect(text[:curr])
        curr += 100

    return lang


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def get_entities_polyglot(text):
    """
    Extracts named entities from text.

    :return: <list> annotated with polyglot tags.
             Entity structure: {
                 "entity": <str>,
                 "label": <str>
                 }
    """
    try:
        if text.entities:
            return [RecordDict(entity=" ".join(x), label=x.tag)
                    for x in text.entities]

    except ValueError as exc:
        if process_absent_corpus_polyglot(str(exc)):
            raise exc

    except tenacity.RetryError as retry_error:
        exc = retry_error.args[0]._exception
        if type(exc) == ValueError:
            if process_absent_corpus_polyglot(str(exc)):
                raise exc

    except Exception:
        # An unspecified exception leads to empty result.
        pass

    # Nothing helped...
    return []


def get_entities_spacy(text, **kwargs):
    """
    Extracts named entities from text.

    :return: <list> annotated with spaCy labels.
    """
    lang = str(kwargs.get('lang', 'xx')).strip()
    Model().ensure_model(f'spacy.{lang}')
    doc = Model().spacy[lang](text)
    return [RecordDict(entity=x.text, label=x.label_) for x in doc.ents]


class ParamsError(Exception):
    pass


class Extractor():
    """
    Extracts named entities and/or places from text.

    Use:
        xtr = Extractor(<str>)
        xtr.entities # list of labelled entities
        xtr.places # list of places
    """
    def __init__(self, text, **kwargs):
        """
        :param text: <str>
        :kwarg lang: <str> 2 digits language code
        :kwarg ner: <str>
        """
        self.PLACE_LABELS = [
            "I-LOC", "I-ORG", # Polyglot
            "FAC", "GPE", "LOC", "PERSON" # spaCy
            ]
        self._text = text
        self._lang = kwargs.get('lang', detect_lang(text, default='xx'))

        ner = kwargs.get('ner', None)
        NER_def = self._NER()
        if ner is None:
            NERs = NER_def
        else:
            ner = str(ner).strip().lower()
            ner_available = NER_def.keys()
            if ner not in ner_available:
                raise ParamsError(
                    'NER `{}` not supported! Use one of the following: {}'.format(
                        ner, ', '.join(ner_available)))

            NERs = {ner: NER_def[ner]}

        self._entities = []
        for ner_ in NERs:
            text_ner = NER_def[ner_].get_text(text)
            options = {}
            if not self._lang:
                # Chances are that Polyglot already detected this...
                try:
                    self._lang = text_ner.language.code
                except AttributeError:
                    pass

            if self._lang:
                options.update(lang=self._lang)

            entities = NER_def[ner_].get_entities(text_ner, **options)
            self._entities.extend(entities)

        self._places = self.get_places()

    @objectify
    def _NER(self):
        return {
            'polyglot': {
                'get_text': lambda x: Text(str(x)),
                'get_entities': get_entities_polyglot,
                },
            'spacy': {
                'get_text': lambda x: x,
                'get_entities': get_entities_spacy,
                }
            }

    def get_lang(self, **kwargs):
        lang = kwargs.get('lang', None)
        if lang:
            return str(lang).strip().lower()

        try:
            return self._text_ner.language.code
        except AttributeError:
            pass

        return 'xx'

    def get_places(self):
        """
        Extracts locations from named entities. Returns only
        superstrings (if `places` contain "Kom" and "Kom el-Shuqafa",
        only the latter will be included in the result).
        """
        places = [x.entity for x in self.entities
                  if x.label in self.PLACE_LABELS]
        return [j for i, j in enumerate(places)
                if all(j not in k for k in places[i + 1:])]

    @property
    def text(self):
        return self._text

    @property
    def lang(self):
        return self._lang

    @property
    def entities(self):
        return self._entities

    @property
    def places(self):
        return self._places
