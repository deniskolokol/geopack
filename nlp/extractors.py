# -*- coding: utf-8 -*-

"""
This module contains functions and classes for extracting
place names (and, more generally, named entitites) from text.
"""

import tenacity
from polyglot.text import Text
from polyglot.detect import Detector
import spacy
import spacy_fastlang

from genery.utils import smart_truncate, objectify, RecordDict, TextCleaner

from conf import settings
from nlp.models import Model, process_absent_corpus_polyglot


def detect_lang(text: str, engine: str = 'polyglot', default=None) -> str:
    """
    Sniffs the language and returns its code (or None, if unsuccessfull).

    Default engine is 'polyglot' because:
    - it is much faster than spaCy
    - doesn't require loading of any model
    """
    def _do_detect(text, engine, detector):
        txt = TextCleaner(text).cleanup_hard()
        if engine == 'polyglot':
            try:
                detector = Detector(txt)
            except Exception:
                return default
            return detector.language.code

        elif engine == 'spacy':
            return detector(txt)._.language

        else:
            return default

    if engine == 'spacy':
        detector = Model().spacy[Model._lang_default]
        if not any(isinstance(x[1], spacy_fastlang.LanguageDetector)
                   for x in detector.pipeline):
            detector.add_pipe('language_detector', last=True)
    elif engine == 'polyglot':
        detector = Detector

    # Avoid big texts.
    lim = 1000
    if len(text) <= lim:
        return _do_detect(text, engine, detector)

    curr = 200
    lang = None
    while (not lang) or (curr <= lim):
        lang = _do_detect(smart_truncate(text, limit=curr, suffix=''),
                          engine,
                          detector)
        curr += 200

    return lang


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def get_entities_polyglot(text, **kwargs):
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

    # XXX: deal with it! monitoring, tests, the usual...
    #
    # except Exception:
    #     # Any unspecified exception leads to empty result.
    #     pass

    # Nothing helped...
    return []


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

    _place_labels = ["FAC", "GPE", "LOC", "ORG"]

    def __init__(self, text, **kwargs):
        self._entities = None
        self._places = None

        self._lang = kwargs.get('lang', detect_lang(text, default='xx'))
        self._text = text

        Model().ensure_model(f'spacy.{self._lang}')
        self._nlp = Model().spacy[self._lang]
        self._doc = self._nlp(text)

    def get_entities(self) -> list:
        """
        Extracts named entities from text.

        :return: <list> annotated with spaCy labels.
        """
        stop_words = None
        try:
            lang_module = getattr(spacy.lang, self.lang)
            stop_words = lang_module.stop_words.STOP_WORDS
        except AttributeError:
            pass

        entities = []
        for ent in self.doc.ents:
            ent = {
                'text': ent.text,
                'label': ent.label_,
                'start_char': ent.start_char,
                'end_char': ent.end_char
                }
            if stop_words:
                ent = self._lstrip_stop_words(ent, stop_words)
            entities.append(RecordDict(**ent))

        return entities

    def get_places(self):
        return [x for x in self.entities if x.label in self._place_labels]

    def _lstrip_stop_words(self, entity: dict, stop_words: (set | list | tuple)) -> dict:
        """
        Removes stop words from left side (mainly articles).
        Re-calculates entity['start_char'].

        :param entity: <dict> in the form:
            {
                'text': <str>,
                'label': <str>,
                'start_char': <int>,
                'end_char': <int>
            }
        :param stop_words: <set | list | tuple>
        :return: <dict> in the same form as `entity`.
        """
        chunks = entity['text'].split(' ', 1)
        while chunks[0] in stop_words:
            try:
                entity.update({'text': chunks[1]})
            except IndexError:
                break
            else:
                entity['start_char'] += (len(chunks[0]) + 1)

            chunks = entity['text'].split(' ', 1)

        return entity

    @property
    def text(self): return self._text

    @property
    def doc(self): return self._doc

    @property
    def nlp(self): return self._nlp

    @property
    def lang(self): return self._lang

    @property
    def entities(self):
        if self._entities is None:
            self._entities = self.get_entities()

        return self._entities

    @property
    def places(self):
        if self._places is None:
            self._places = self.get_places() if self.entities else []

        return self._places
