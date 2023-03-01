# -*- coding: utf-8 -*-

"""GeoPack Web server."""

import asyncio
import json
import logging

from tornado.web import url, Application, RequestHandler, HTTPError

from genery.utils import RecordDict

from conf import settings
from geo.parsers import GeoParser


LOG = logging.getLogger("root")
STATUS_OK = 'OK'
API_CONF = RecordDict(**settings.API_SCHEMA)


NO_SUCH_ACTION = "Undefined action: `{}`"


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
        bearer_token = self.request.headers.get('Authorization', '')
        bearer_token = bearer_token.replace('Bearer', '').replace('bearer', '')
        bearer_token = bearer_token.strip()
        if bearer_token != settings.GEOPACK_TOKEN:
            raise HTTPError(
                403,
                reason="Authentication failed: bearer token is invalid or absent!"
                )

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

        return {endpoint_name: self.data}


class APIGeoJSONHandler(APIHandler):
    def __init__(self, *args, **kwargs):
        # Assume required params should also become internal
        # params for all GeoJSON API handlers, e.g. required param
        # "text" becomes `self.text`.
        # WARNING: Required params should not cross names with system
        # internal params (i.e. no such params as 'name', 'class', etc.)
        self.internal_params = self._meta.required_params
        super().__init__(*args, **kwargs)

    def post(self, *args, **kwargs):
        success = self.fill_data()
        if not success:
            raise HTTPError(400, reason=self.data)

        resp = self.prepare_response()
        self.write(resp)

    def prepare_response(self):
        """Re-format to GeoJSON."""
        return {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "EPSG:4326"
                    }
                },
            "features": self.data
        }

    def fill_data(self):
        """Implement it locally in handlers."""
        raise NotImplementedError()


class APIGeoTagHandler(APIGeoJSONHandler):
    """
    Geo-tagging API: extracts geo-locations from free text
    (i.e. requests params must include `text`) and return
    all places found with coordinates and descriptions in
    GeoJSON format.
    """
    class Meta:
        required_params = ['text']

    def fill_data(self, *args, **kwargs):
        kw = {"include_region": False}
        kw.update(self.json_args)
        try:
            self.data = GeoParser().parse(self.text, **kw)
        except Exception as exc:
            self.data = exc
            return False

        return True


class APIGeoPlaceHandler(APIGeoJSONHandler):
    """
    API for search and geo-locate for a place by its name
    (i.e. requests params must include `query`) and return
    top 5 results with coordinates and descriptions in
    GeoJSON format.
    """
    class Meta:
        required_params = ['query']

    def fill_data(self, *args, **kwargs):
        self.data = {}
        #TODO: call search and geotagger here with **self.json_args
        return True


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
        url(r"/api/geotag/", APIGeoTagHandler, name="api_geotag"),
        url(r"/api/geoplace/", APIGeoPlaceHandler, name="api_geoplace"),
        ]

    return Application(urls, autoreload=True)


async def main():
    LOG.debug('Server accepting requests at %d', settings.GEOPACK_SERVER_PORT)

    app = make_app()
    app.listen(settings.GEOPACK_SERVER_PORT)
    shutdown_event = asyncio.Event()
    await shutdown_event.wait()


if __name__ == "__main__":
    asyncio.run(main())
