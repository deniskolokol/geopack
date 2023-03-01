# -*- coding: utf-8 -*-

"""
This module contains classes for geoparsing text, i.e. taking text as an input
it returns coordinates for each of geo-locations found in the text.
"""

from genery.utils import TextCleaner, flatten_list

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
    In [1]: geo = GeoParser()
    In [2]: result = geo.parse("Flooding in Canada with water rescues ongoing. They are using boats also. # Ottawa, Cambridge Bay of Nunavut and Tarbutt", lang="en")
    """
    class Meta:
        queryset = Place.search
        # Fields to extract from Elasticsearch.
        source = [
            'name',
            'placetype',
            'belongsto',
            'hierarchy',
            'location',
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
        fields = [
            '_id',
            '_score',
            'place', # name by Extractor
            'name',
            'placetype',
            'belongsto',
            'location',
            'region',
            'iso_country',
            'country',
            'area',
            'area_square_m',
            'geomhash',
            'location',
            'timezone',
            'population',
            'geometry',

            # fields that come from Entity
            'text',
            'start_char',
            'end_char',
            'label'
            ]
        sort = ['-population']
        plimit = 10 # limit for a query on a single place

    def __init__(self):
        self._xtor = None
        self.include_region = False

    def parse(self, text, **kwargs):
        """
        Parses `text` and returns list of found locations.

        :return: <list> of <dict>
        """
        self.include_region = kwargs.pop('include_region', False)
        self._xtor = Extractor(text, **kwargs)
        result = []

        # The place can occur several times in the text,
        # don't waste query for it, save it for re-use.
        places_found = {}
        for place in self._xtor.places:
            place_name = TextCleaner(place.text).cleanup_hard()
            try:
                found = places_found[place_name]
            except KeyError:
                found = self.search_place(place_name, topn=1)
                if found:
                    found = found[0]
                else:
                    found = {}
                # Count empty results, too.
                places_found[place_name] = found

            if found:
                result.append(self.dehydrate({**found, **place}))

        return result

    def search_place(self, place, **kwargs):
        """
        Direct request to the gazetteer in Elastcisearch index.

        WARNING: `lang` parameter makes search more precise, but also
                 substantially slower. If possible, avoid it - search
                 will still be performed on the `names` fields (which
                 is multi-lang).

        :param place: <str>
        :return: <list> of <dict>
        """
        field_names = ["name", "names"]
        lang = kwargs.get("lang", None)
        topn = kwargs.get("topn", False)
        if lang:
            field_names.append("names_lang.{}".format(lang))
        query = {
            "query": {
                "multi_match": {
                    "query": place,
                    "fields": field_names
                }
            },
            "_source": self._meta.source,
            "size": self._meta.plimit,
        }
        qs = self._meta.queryset().from_dict(query)

        return [self.prepare(rec) for rec in qs][:topn]

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

    def dehydrate(self, rec):
        """Prepares resulting record."""
        rec_ = {}
        for field in self._meta.fields:
            try:
                rec_[field] = rec[field]
            except KeyError:
                rec_[field] = None

        return rec_

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

    def _filter_places(self, hits, locs, place_locs):
        """
        Excluds higher elements in hierarchy, e.g., if `hits`
        contains localities and a region, to which those
        localities belong, leave only localities.

        :param hits: <dict> {
            _id <str>: hit <dict>
            },
            where `hit` is a search result
        :param locs: <dict> {
            _id <str>: belongs_to <list> of <int>
            }
        :param place_locs: <dict> {
            place_name <str>: hits_ids <list> of <str>
            }

        :return: <list> of <str>
        """
        ids_belongsto = set(flatten_list(locs.values()))
        ids_hilevel = [int(k) for k in hits.keys() if int(k) in ids_belongsto]

        # Remove place branch entirely if any of the hits is in
        # the list of places that include other places (e.g. if
        # Ottawa and Canada both found, remove Canada and everything
        # that found along, i.e. "Canada Bay", etc.).
        filtered_places = {}
        for place, loc_ids in place_locs.items():
            if any(int(_id) in ids_hilevel for _id in loc_ids):
                continue

            filtered_places.update({place: loc_ids})

        # Use only top scored hits from `places_clean`.
        result = []
        filtered_hits = {}

        places_tmp = {}

        for place_name, place_hits in filtered_places.items():
            _scores = {}
            _tokens = {}
            for key, hit in hits.items():
                if key not in place_hits:
                    continue

                belongsto_names = [x['name'] for x
                                   in self.resolve_belongsto(hit)]
                filtered_hits[key] = hits[key]
                filtered_hits[key].update({'belongsto': belongsto_names})

                _scores.update({key: filtered_hits[key]['_score']})
                _tokens.update({key: _get_tokens(filtered_hits[key],
                                                'belongsto',
                                                'name')})

            places_tmp[place_name] = _scores

            #TODO: instead of similarity, calculate graph of dictances
            # between all points and choose the top one for each found
            # place in filtered_places (algo?)
            #
            # Use: utils.haversine

            # Obtain hits similarities for a current place
            # and use top two for boosting the "_score".
            #
            # Top two is essentially a pair of the most similar
            # items among the highest ranked in search - therefore
            # they will be boosted equally. This will separate them
            # from the rest (least similar - like locations in other
            # regions or countries). Then search _score alone will
            # decide the winner.
            similarity = Extractor.similarity(
                _tokens, **{"threshold": 0.9, "limit": 10, "lang": self.lang}
                )
            for rec in similarity[:2]:
                _scores[rec["id1"]] *= (rec["similarity"] * 10)

            # Convert _scores to <list>, sorting.
            _scores = [{"key": k, "_score": v} for k, v in _scores.items()]
            _scores = sorted(_scores, key=lambda x: x["_score"], reverse=True)

            try:
                result.append(hits[_scores[0]["key"]])
            except (KeyError, IndexError):
                continue

        return result

    def parse_places(self, places, **filters):
        """
        Search for each place in `places` and returns list of found locations.

        :param places: <list>
        :return: <list> of <dict>
        """
        # Places found in index (hits) with the full info
        # {hit_id: {data}}.
        hits = {}

        # Reduced version for obtaining similarity.
        locs = {}

        # Place names vs. hits' ids (used for removing all hits
        # for a certain place name if any of them includes other
        # places).
        place_locs = {}
        for place in places:
            for hit in self.search_place(place, **filters):
                hit.update(place=place)
                hits.update({hit["_id"]: hit})

                try:
                    place_locs[place].append(hit["_id"])
                except KeyError:
                    place_locs[place] = [hit["_id"]]

                loc = []
                try:
                    loc = [int(bt) for bt in hit["belongsto"]]
                except KeyError:
                    pass
                if loc:
                    locs.update({hit["_id"]: loc})

        # In case of a single result, all the following jazz is unnesessary.
        if len(hits) == 1:
            return [self.dehydrate(list(hits.values())[0])]

        hits_filtered = self._filter_places(hits, locs, place_locs)

        return [self.dehydrate(hit) for hit in hits_filtered]

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
    def lang(self):
        return self._lang

    @property
    def xtor(self):
        return self._xtor

    @property
    def _meta(self):
        return self.Meta()
