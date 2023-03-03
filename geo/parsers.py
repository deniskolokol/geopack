# -*- coding: utf-8 -*-

"""
This module contains classes for geoparsing text, i.e. taking text as an input
it returns coordinates for each of geo-locations found in the text.
"""

from genery.utils import TextCleaner, flatten_list, distinct_elements
from genery.decorators import timeit

from src.utils import country_name
from nlp.extractors import Extractor
from geo.documents import Place


def _get_tokens(rec, *fields, **opts):
    """
    Concatenates values from `fields` in rec,
    using opts['delimiter'] (default: comma).
    """
    delimiter = opts.get('delimiter', ',')
    values = []
    for field in fields:
        try:
            val = rec[field]
        except KeyError:
            continue

        if not isinstance(val, (list, tuple)):
            val = [val]
        values.extend(val)

    return delimiter.join(values)


class GeoParser():
    """
    Examples of use:
    In [1]: geo = GeoParser("Flooding in Canada with water rescues ongoing. They are using boats also. # Ottawa, Cambridge Bay of Nunavut and Tarbutt", lang="en")
    In [2]: print(geo.data)
    """
    class Meta:
        queryset = Place.search
        # Fields to extract from Elasticsearch.
        source = [
            'name',
            'placetype',
            'belongsto',
            'hierarchy',
            'iso_country',
            'country',
            'area',
            'area_square_m',
            'geomhash',
            'location',
            'timezone',
            'population',
            'geometry'
            ]
        # Fields in the resulting object.
        properties = [
            '_id',
            '_score',
            'name',
            'placetype',
            'belongsto',
            'location',
            'iso_country',
            'country',
            'area',
            'area_square_m',
            'geomhash',
            'location',
            'timezone',
            'population',
            'hierarchy',

            # fields that come from Entity
            'text',
            'start_char',
            'end_char',
            'label'
            ]

        # Boost search results that are bigger cities.
        sort = ['-population']

        # Default limit for a query on a single place.
        plimit = 10

    def __init__(self, text, **kwargs):
        self._xtor = None
        self._text = text
        self.include_region = kwargs.pop('include_region', False)
        self._lang = kwargs.get('lang', None)
        self._topn = kwargs.get('topn', self._meta.plimit)
        self._data = None
        self._query = None

        #TODO:
        # - def fill_regions(self), which injects regions into self.data
        # - def fill_hierarchy(self), which injects into self.data
        #   geo-names instead of ids

    @timeit
    def parse(self):
        """
        Parses `text` and returns list of found locations.

        :return: <list> of <dict>
        """
        kwargs = {}
        if self.lang:
            kwargs = {'lang': self.lang}
        self._xtor = Extractor(self.text, **kwargs)
        self._lang = self._xtor.lang

        result = []

        # The place can occur several times in the text,
        # don't waste query for it, save it for re-use.
        places_found = {}
        for place in self._xtor.places:
            place_name = TextCleaner(place.text).cleanup_hard()
            try:
                found = places_found[place_name]
            except KeyError:
                found = self.search_place(place_name)
                if found:
                    found = found[0]
                else:
                    found = {}
                # Count empty results, too.
                places_found[place_name] = found

            if found:
                result.append(self.dehydrate({**found, **place}))

        # Update self._data.
        self._data = result

        return result

    def search_place(self, place: str) -> list:
        """
        Direct request to the gazetteer in Elastcisearch index.

        WARNING: `lang` parameter makes search more precise, but also
                 substantially slower. If possible, avoid it - search
                 will still be performed on the `names` fields (which
                 is multi-lang).

        :param place: <str>
        :return: <list> of <dict>
        """
        try:
            self.query['query']['multi_match']['query'] = place
        except (TypeError, KeyError):
            self.query = {
                 'query': {
                    'multi_match': {
                        'query': place,
                        'fields': ['name', 'names'],
                        'type': 'phrase_prefix'
                    }
                },
                'sort': self._query_sort(),
                'size': self.topn,
                '_source': self._query_source(),
            }
        return [self.prepare(rec) for rec
                in self._meta.queryset().from_dict(self.query)]

    def _query_sort(self, **args):
        conditions = self._meta.sort + list(args)
        conditions.append('_score')
        conditions = distinct_elements(conditions, preserve_order=True)

        sort = []
        for cond in conditions:
            if cond.startswith('-'):
                sort.append({cond[1:]: 'desc'})
            else:
                sort.append(cond)

        return sort

    def _query_source(self):
        _source = self._meta.source[:]
        if self._lang not in (None, 'xx'):
            _source.append(f'names_lang.{self.lang}')

        return _source

    def prepare(self, rec):
        """
        :return: <dict> {
            (key, value) pairs according to `_source`
            belongsto: <list> of <str>,
            _id: <str> or <int>,
            _score: <float>
            }
        """
        obj = rec.to_dict()
        if self.include_region:
            belongsto = self.resolve_belongsto(obj)
            obj.update(region=self.get_region(obj, belongsto))

        obj.update({
            "_id": rec.meta.id,
            "lat": rec["location"]["lat"],
            "lon": rec["location"]["lon"],
            })
        try:
            obj.update({"_score": rec.meta.score})
        except KeyError:
            obj.update({"_score": 0.})

        return obj

    def dehydrate(self, rec: dict) -> dict:
        """
        Prepares resulting record:
        - makes sure all fields are present (even if val is None)
        - re-format obj to meet GeoJSON format.
        """
        result = {'type': 'Feature'}
        properties = {}
        for field in self._meta.properties:
            try:
                properties[field] = rec[field]
            except KeyError:
                properties[field] = None

        result.update(properties=properties,
                      geometry=rec['geometry'])
        return result

    def parse_place(self, text, **filters):
        result = []
        hits = self.search_place(text, **filters)
        try:
            hit = hits[0]
        except IndexError:
            # Nothing found.
            pass
        else:
            hit.update(place=text)
            result = [self.dehydrate(hit)]
        finally:
            return result

    def build_query(self, place):
        fields = ["name", "names"]
        if self.lang and self.lang != "xx":
            fields.append("names_lang.{}".format(self.lang))
        return {"multi_match": {"query": place, "fields": fields}}

    def query(self, match, source=None, sort=None):
        """
        :param match: <dict>
        :param source: <list> or None
        :param sort: <list> or None
        """
        qs = self._meta.queryset().query("match", **match) \
                       .source(source or self._meta.source)
        if sort:
            qs = qs.sort(*sort)
        return qs

    def resolve_belongsto(self, rec):
        """
        :param rec: <dict>
        """
        belongsto = []
        ids = rec.get('belongsto', None)
        if not ids:
            return belongsto

        qs = self._meta.queryset().filter('terms', _id=ids).source("name")
        qs = dict((rec.meta.id, rec.name) for rec in qs if rec.name)

        # Must save the order of `belongsto`!
        result = []
        for _id in ids:
            try:
                result.append({"id": _id, "name": qs[str(_id)]})
            except KeyError:
                pass
        return result

    def get_hierarchy_items(self, rec, item_type, belongs_to):
        """
        Returns name of item from hierarchy according to its type
        (e.g., 'country', 'county', 'region', etc.)
        """
        belongs_to = belongs_to or []
        try:
            hierarchy = rec["hierarchy"][0]
        except (KeyError, IndexError):
            return None

        try:
            hierarchy_id = hierarchy[item_type+"_id"]
        except KeyError:
            return None

        # First look in belongs_to - we might already have a name.
        belongs_to = dict((x["id"], x["name"]) for x in belongs_to)
        try:
            return belongs_to[hierarchy_id]
        except KeyError:
            pass

        # Lookup in the index.
        qs = self.query(match={"_id": hierarchy_id}, source=["name"])
        try:
            item = [x for x in qs][0].to_dict()
        except (IndexError, AttributeError):
            return None
        else:
            return item['name']

    def get_region(self, rec, belongs_to):
        """
        Searches for region name depending on placetype.

        :param rec: <dict>
        :param belongs_to: <list> of <dict>
        """
        if rec["placetype"] in [
                "county",
                "metro area",
                "locality",
                "macrohood",
                "neighbourhood",
                "microhood",
                "campus",
                "building",
                "address",
                "venue"
            ]:
            # "county" is a priority, but an optional placetype.
            name = self.get_hierarchy_items(rec, "county", belongs_to)
            if not name:
                name = self.get_hierarchy_items(rec, "region", belongs_to)
            return name

        if rec["placetype"] in [
                "empire",
                "country",
                "macroregion"
            ]:
            # Downshifting "empire" to "country".
            self.get_hierarchy_items(rec, "country", belongs_to)

        # For other placetypes, try to return name of "region".
        region = self.get_hierarchy_items(rec, "region", belongs_to)

        # If everything else failed, consider the place itself as a region.
        if region is None:
            region = rec["name"]

        return region

    @property
    def _meta(self): return self.Meta()

    @property
    def lang(self): return self._lang

    @property
    def xtor(self): return self._xtor

    @property
    def text(self): return self._text

    @property
    def topn(self): return self._topn

    @property
    def data(self):
        # Call for self.parse only if self._data == None,
        # i.e. instance initialized, but not yet porocessed
        # (processing can result in empty list, in which case
        # re-processing the same text is a waste of resources).
        if self._data is None:
            self.parse()

        return self._data
