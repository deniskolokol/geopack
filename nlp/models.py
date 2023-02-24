# -*- coding: utf-8 -*-

"""Model managers for languages and packages."""

import logging

import spacy
import spacy_udpipe
from polyglot.downloader import downloader

from genery.utils import RecordDict

from conf import settings


LOG = logging.getLogger("root")
TIMEOUT = 30
NOT_SUPPORTED_MSG = lambda x: f'The model `{x}` is not supported.'


def process_absent_corpus_polyglot(exc):
    """Downloads absent corpus for Polyglot."""
    if exc.startswith("This resource is available in the index but not downloaded, yet."):
        command = exc.split("\n")[-1]
        package = command.rsplit(" ", 1)[-1]
        downloader.download(package)
        return True
    else:
        # "Package 'xxx.yy' not found in index."
        return False


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

    # Default language
    _lang_default = 'en'

    # Model names for languages in the form {<lang>: <package_name>}.
    # NB: the models available via spacy-udpipe should have a
    # corresponding constant as a key.
    _udpipe = 'udpipe'
    _spacy_lang_model = {
        'cs': _udpipe,
        'da': 'da_core_news_sm',
        'de': 'de_core_news_sm',
        'el': 'el_core_news_sm',
        'en': 'en_core_web_sm',
        'es': 'es_core_news_sm',
        'fi': 'fi_core_news_sm',
        'fr': 'fr_core_news_sm',
        'it': 'it_core_news_sm',
        'lt': 'lt_core_news_sm',
        'lv': _udpipe,
        'mk': 'mk_core_news_sm',
        'nl': 'nl_core_news_sm',
        'pl': 'pl_core_news_sm',
        'pt': 'pt_core_news_sm',
        'ru': 'ru_core_news_sm',
        'sk': _udpipe,
        'sv': 'sv_core_news_sm',
        'uk': 'uk_core_news_sm',
        'xx': 'xx_ent_wiki_sm'
        }

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, *args, **kwargs):
        """
        Initializes attributes (holders), the models are being loaded
        on lazily on demand, except of the model for _lang_default,
        which is ensured on load.

        Every holder is a dictionary in the form:
        {`lang` <str>: `model` <...>}
        """
        # The instance could already be initialized.
        try:
            self.spacy
        except AttributeError:
            # Set up the structure: empty containers for loading
            # model from files or URLs.
            spacy_langs = dict((l, None) for l
                               in self._spacy_lang_model.keys())
            self.spacy = RecordDict(**spacy_langs)

        self.ensure_model(f'spacy.{self._lang_default}')

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

        # NB: add `elif` for additional engines if/when necessary.
        if engine == 'spacy':
            lang = path_
            if self.spacy[lang]:
                # The model is in place.
                return

            if lang in self._spacy_lang_model:
                package = self._spacy_lang_model[lang]
            else:
                package = self._spacy_lang_model['xx']

            if package == self._udpipe:
                func_load = spacy_udpipe.load
                func_download = spacy_udpipe.download
            else:
                func_load = spacy.load
                func_download = spacy.cli.download

            try:
                model = func_load(package)
            except OSError:
                LOG.warning('Downloading language model `%s` for spaCy NER ' +\
                            '(this will only happen once)...',
                            package
                    )
                func_download(package)
                model = func_load(package)

            self.spacy[lang] = model
