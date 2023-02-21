# -*- coding: utf-8 -*-

"""Model managers for languages and packages."""

import logging

from genery.utils import RecordDict

from conf import settings
from nlp.loaders import get_spacy_ner_model


LOG = logging.getLogger("root")
TIMEOUT = 30
NOT_SUPPORTED_MSG = lambda x: f'The model `{x}` is not supported.'


class NotSupportedModelError(Exception):
    pass


class Model:
    """
    A singleton for all langauges and models.

    Examples of usage:
        model = Model().spacy.en

        lang = 'es'
        model = Model().spacy[lang]

    This will ensure that the language model is downloaded
    from the corresponding repository and is available as a
    property.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, *args, **kwargs):
        """
        Only initialize attributes (holders) - the models are
        being loaded on demand.
        Every holder is a dictionary in the form:
        {`lang` <str>: `model` <...>}
        """
        # The instance maight already be initialized.
        try:
            self.spacy
        except AttributeError:
            # Set up the structure: empty containers for loading
            # model from files or URLs.
            spacy_langs = dict((l, None) for l
                               in settings.SPACY_LANG_MODEL.keys())
            self.spacy = RecordDict(**spacy_langs)

    def ensure_model(self, path: str):
        """
        Example:
        Model().ensure('spacy.en')

        :param path: <str> consists of properties divided by dot, e.g.
            'spacy.en' or 'bert.large'
        :return: <bool> True if model is present, raises
            NotSupportedModelError otherwise.
        """
        engine, path_ = path.split('.', 1)

        # `-1` means unsupported (in the beginning all models are None).
        model = -1
        engine_attr = getattr(self, engine)
        try:
            model = engine_attr.lookup(path_, default=-1)
        except (TypeError, KeyError, AttributeError, ValueError):
            pass

        if model == -1:
            raise NotSupportedModelError(NOT_SUPPORTED_MSG(path))

        if engine == 'spacy':
            self.spacy[path_] = get_spacy_model(path_)
        else:
            raise NotSupportedModelError(NOT_SUPPORTED_MSG(path))
