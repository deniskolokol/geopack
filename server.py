# -*- coding: utf-8 -*-

"""GeoPack Web server."""

import asyncio
import json
import logging

from tornado.web import url, Application, RequestHandler, HTTPError

from genery.utils import RecordDict

from conf import settings
# from nlp.models import Model


LOG = logging.getLogger("root")
STATUS_OK = 'OK'
API_CONF = RecordDict(**settings.API_SCHEMA)


# Functions that do not require creation of TextProcessor
# or TextAnalyzer.
FUNCS = ['custom_model', 'keyphrases', 'named_entities']
# Modules to look actions for.
MODULES = ('nlp.processors', 'nlp.extractors',)
NO_SUCH_ACTION = "Undefined action: `{}`"


def get_kwargs(**params):
    """
    Separates kwargs for an instance and its method.

    :return: (<dict>, <dict>)
    """
    inst_kwarg_names = ('custom_model', 'max_length')

    method_kwargs = {}
    cls_kwargs = {}
    for kw, val in params.items():
        if kw in inst_kwarg_names:
            cls_kwargs.update({kw: val})
        else:
            method_kwargs.update({kw: val})

    return cls_kwargs, method_kwargs


def get_class(action):
    TextProcessorClass = None
    for class_ in (TextProcessor, TextAnalyzer, Trainer):
        try:
            getattr(class_, action)
        except AttributeError:
            continue
        else:
            TextProcessorClass = class_

    return TextProcessorClass


def get_method(action, instance):
    """
    :param action: <str>
    :param instance: <cls>

    :return: <func>
    """
    try:
        return getattr(instance, action)
    except AttributeError:
        return None


def clean_action_params(action_def):
    if not isinstance(action_def, (str, dict)):
        err_msg = \
          "Pipeline item should be <str> or <dict>, currently {} ({})" \
          .format(type(action_def).__name__, str(action_def))
        raise TypeError(err_msg)

    if isinstance(action_def, str):
        return action_def, {}

    try:
        action = action_def['action']
    except KeyError as exc:
        msg = "Pipeline item should include `action`. Current value: {}" \
              .format(str(action_def))
        raise KeyError(msg) from exc

    params = action_def.get("params", {})
    if not isinstance(params, dict):
        try:
            params = json.loads(params)
        except json.JSONDecodeError as exc:
            msg = "`params` should be proper JSON: {}".format(str(params))
            raise TypeError(msg) from exc

    return action, params


def _do_process_text(text, lang, action, **params):
    method = None
    if action in FUNCS:
        for module in MODULES:
            try:
                method = getattr(__import__(module), action)
            except Exception as err:
                pass

        if not method:
            return False, NO_SUCH_ACTION.format(action)

        kwargs_process = params.copy()
        # `lang` isn't required in functions and resides in kwargs.
        if lang:
            kwargs_process.update(lang=lang)
    else:
        # For class methods process instance's and method's kwargs separately.
        kwargs_init, kwargs_process = get_kwargs(**params)
        TextProcessorClass = get_class(action)
        if not TextProcessorClass:
            return False, NO_SUCH_ACTION.format(action)

        TextProcessorInstance = TextProcessorClass(lang=lang, **kwargs_init)
        method = get_method(action, TextProcessorInstance)

    try:
        processed = method(text, **kwargs_process)
    except Exception as err:
        return False, f'{err} ({type(err).__name__})'

    return True, processed


def process_text(text: str, pipeline: list, **kwargs) -> list:
    """Text processing pipeline - proxy func."""
    lang = kwargs.pop('lang', None)
    result = []
    for action_def in pipeline:
        try:
            action, params = clean_action_params(action_def)
        except Exception as exc:
            result.append({
                'action': str(action_def),
                'success': False,
                'data': {"error": "{}: {}".format(type(exc).__name__, str(exc))}
            })
            continue

        success, data = _do_process_text(text, lang, action, **params)
        if not success:
            data = {"error": data}
        result.append({'action': action, 'success': success, 'data': data})

    return result


def get_name_from_path(path: str) -> str:
    """
    Extracts the name from URI (last bit).

    :param path: <str> in the form of '/api/vN/polarity/'
    :return: <str> - 'polarity'
    """
    return [x for x in path.split('/') if x.strip()][-1].lower()


class APIHandlerOptions:
    """
    A configuration class for APIHandler.

    Provides sane defaults and the logic necessary
    to start with the internal class Meta used on
    any of APIHandler subclasses.
    """
    allowed_methods = []
    required_params = []
    endpoint_name = ''
    method = print

    def __new__(cls, meta=None):
        overrides = {}

        # Handle overrides.
        if meta:
            for override_name in dir(meta):
                # Internals are discarded!
                if not override_name.startswith('_'):
                    overrides[override_name] = getattr(meta, override_name)

        allowed_methods = overrides.get(
            'allowed_methods', ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
        overrides['allowed_methods'] = allowed_methods

        return object.__new__(type('APIHandlerOptions', (cls,), overrides))


class APIHandler(RequestHandler):
    class Meta:
        # Only GET and POST allowed by default.
        allowed_methods = ('GET', 'POST',)

    def __new__(cls, application, request, **kwargs):
        instance = super(APIHandler, cls).__new__(cls, **kwargs)
        opts = getattr(instance, 'Meta', None)
        instance._meta = APIHandlerOptions(opts)

        if not getattr(instance._meta, 'endpoint_name', None):
            # No `endpoint_name` provided. Attempt to auto-name the endpoint.
            instance._meta.endpoint_name = get_name_from_path(request.path)

        return instance

    def __init__(self, application, request, **kwargs):
        self.data = RecordDict()
        self.json_args = {}
        try:
            self.internal_params.append('token')
        except AttributeError:
            self.internal_params = ['token']

        # Initialize internal params.
        for param in self.internal_params:
            setattr(self, param, None)

        super().__init__(application, request, **kwargs)

    def get(self, *args, **kwargs):
        #TODO: respond with schema (parameters), that should be
        #      used in POST requests
        # NB: all endpoints should respond with data only to POST.
        endpoint_name = get_name_from_path(self.request.path)
        self.write(API_CONF.endpoints[endpoint_name])

    def set_default_headers(self):
        self.set_header("Content-Type", 'application/json')

    def validate_method(self):
        if self.request.method not in self._meta.allowed_methods:
            allowed_methods = ', '.join(self._meta.allowed_methods)
            raise HTTPError(400,
                reason=f"Wrong method `{self.request.method}`! Allowed are: {allowed_methods}.")

    def initialize(self, *args, **kwargs):
        content_type_accepted = 'application/json'
        if self.request.method == 'POST':
            content_type = self.request.headers.get('Content-Type', '')
            if not content_type.startswith(content_type_accepted):
                raise HTTPError(400,
                    reason=f"Wrong content_type `{content_type}`! Only `{content_type_accepted}` is accepted.")

            self.json_args = json.loads(self.request.body)
            req_params = list(self._meta.required_params)
            req_params.extend(self.internal_params)
            for param in set(req_params):
                try:
                    param_value = self.json_args[param]
                except KeyError:
                    raise HTTPError(400, reason=f"Param `{param}` is required!")
                else:
                    if param in self.internal_params:
                        setattr(self, param, param_value)
                        del self.json_args[param]
        elif self.request.method == 'GET':
            #TODO:
            self.json_args = None

    def authenticate(self):
        if self.token == settings.NLP_TOKEN:
            return True
        raise HTTPError(403,
                        reason="Authentication token is invalid or absent!")

    def prepare(self):
        self.validate_method()
        # Only 'POST' requires authentication.
        if self.request.method == 'POST':
            self.authenticate()

    def write_error(self, status_code, **kwargs):
        msg = ''
        if status_code == 400:
            msg = kwargs['exc_info'][1].reason
            logging.warning(msg)
        elif status_code == 500:
            msg = str(kwargs['exc_info'][1])
            logging.error(msg)

        self.render({"data": msg})

    def prepare_response(self):
        endpoint_name = [x for x in self.request.path.split('/') if x][-1]
        endpoint_name = endpoint_name.strip().lower()
        if endpoint_name == 'actions':
            #TODO:
            return {}

        return {endpoint_name: self.data}


class APITextHandler(APIHandler):
    """
    Handler for all text-based requests (i.e. requests
    that must contain `text` as a parameter).
    """
    class Meta:
        required_params = ['text']

    def __init__(self, *args, **kwargs):
        self.internal_params = ['text']
        super().__init__(*args, **kwargs)

    def post(self, *args, **kwargs):
        action = get_name_from_path(self.request.path)
        json_args = self.json_args.copy()
        lang = json_args.pop('lang', None)

        success, self.data = _do_process_text(self.text,
                                              lang,
                                              action,
                                              **json_args)
        if not success:
            raise HTTPError(400, reason=self.data)

        resp = self.prepare_response()
        self.write(resp)


class APIListHandler(RequestHandler):
    class Meta:
        # Only GET is allowed.
        allowed_methods = ('GET',)

    def get(self, **kwargs):
        endpoints = {}
        for action in API_CONF.endpoints.keys():
            uri = f'/api/{action}/'
            endpoints.update({action: {
                'data': {'method': 'POST', 'uri': uri},
                'schema': {'method': 'GET', 'uri': uri}
                }})
        self.write({'version': API_CONF.version, 'endpoints': endpoints})


class MainHandler(RequestHandler):
    def get(self):
        self.write(f"<h1>Welcome to {settings.PROJECT_NAME}, {settings.PROJECT_MOTTO}</h1>Please use <a href='{settings.PROJECT_DOMAIN}api/'>{settings.PROJECT_DOMAIN}api/</a> for the list of API endpoints.")


def make_app():
    urls = [
        url(r"/", MainHandler),
        url(r"/api/", APIListHandler, name="api_list"),
        ]
    for action in API_CONF.endpoints.keys():
        urls.append(url(f'/api/{action}/', APITextHandler, name=f'api_{action}'))

    return Application(urls, autoreload=True)


async def main():
    LOG.debug('Server accepting requests at %d', settings.GEOPACK_SERVER_PORT)

    app = make_app()
    app.listen(settings.GEOPACK_SERVER_PORT)
    shutdown_event = asyncio.Event()
    await shutdown_event.wait()


def init_setup(*args, **kwargs):
    """Initial setup: environment variables, paths, loading models, etc."""
    LOG.debug('Initializing setup')

    # spaCy
    LOG.debug('Loading default spaCy model for `%s`: `%s`',
              settings.LANG_DEFAULT,
              settings.SPACY_LANG_MODEL[settings.LANG_DEFAULT])
    # Model().ensure_model(f'spacy.{settings.LANG_DEFAULT}')

    LOG.debug('Initializing complete')


if __name__ == "__main__":
    init_setup()
    asyncio.run(main())
