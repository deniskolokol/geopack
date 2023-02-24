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


def lstrip_stop_words(entity: dict, stop_words: (set | list | tuple)) -> dict:
    """
    Removes stop words from left side (mainly articles).
    Re-calculates entity['start_char'].

    :param entity: <dict> in the form:
        {
            'entity': <str>,
            'label': <str>,
            'start_char': <int>,
            'end_char': <int>
        }
    :param stop_words: <set | list | tuple>
    :return: <dict> in the same form as `entity`.
    """
    chunks = entity['entity'].split(' ', 1)
    while chunks[0] in stop_words:
        try:
            entity.update({'entity': chunks[1]})
        except IndexError:
            break
        else:
            entity['start_char'] += (len(chunks[0]) + 1)

        chunks = entity['entity'].split(' ', 1)

    return entity


def get_entities_spacy(text, **kwargs):
    """
    Extracts named entities from text.

    :return: <list> annotated with spaCy labels.
    """
    lang = str(kwargs.get('lang', 'xx')).strip()

    lang_module = getattr(spacy.lang, lang)
    stop_words = None
    try:
        stop_words = lang_module.stop_words.STOP_WORDS
    except AttributeError:
        pass

    Model().ensure_model(f'spacy.{lang}')
    doc = Model().spacy[lang](text)
    entities = []

    for ent in doc.ents:
        ent = {
            'entity': ent.text,
            'label': ent.label_,
            'start_char': ent.start_char,
            'end_char': ent.end_char
            }
        if stop_words:
            ent = lstrip_stop_words(ent, stop_words)
        entities.append(RecordDict(**ent))

    return entities


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
    _place_labels = [
        "I-LOC", "I-ORG", # Polyglot
        "FAC", "GPE", "LOC", "PERSON" # spaCy
        ]
    def __init__(self, text, **kwargs):
        """
        :param text: <str>
        :kwarg lang: <str> 2 digits language code
        :kwarg ner: <str>
        """
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
                  if x.label in self._place_labels]
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
