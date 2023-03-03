# -*- coding: utf-8 -*-

"""
This module contains classes for creating a geo-record in a gazetter.

Data sources:
https://github.com/whosonfirst-data/ (Who's On First)
https://www.gadm.org/ (GADM)
"""

from __future__ import absolute_import
import datetime

from elasticsearch_dsl import connections, Document, InnerDoc, Nested, \
     Keyword, Text, Float, Integer, Date, GeoPoint, GeoShape

from genery.utils import flatten_list, distinct_elements
from genery.decorators import objectify

from conf import settings
from src.exceptions import MissingDataError
from src.utils import country_name
from src.references import LANG_MAP, LANG_FIELDS


ALIAS = 'default'
DEFAULT_GEOMETRY_TYPE = "multipolygon"


# Mapping of fields from Whosonfirst data to the Place
# document in index.
# Warning: only those fields are mentioned here that match
# directly, not requiring any additional logic.
FIELD_MAP = {
    "wof:name": "name",
    "geom:area": "area",
    "geom:area_square_m": "area_square_m",
    "iso:country": "iso_country",
    "wof:country": "country",
    "wof:geomhash": "geomhash",
    "wof:belongsto": "belongsto",
    "wof:hierarchy": "hierarchy",
    "wof:parent_id": "parent_id",
    "wof:placetype": "placetype",
    "wof:tags": "tags"
    }


class Hierarchy(InnerDoc):
    neighbourhood_id = Integer()
    locality_id = Integer()
    metro_id = Integer()
    county_id = Integer()
    region_id = Integer()
    country_id = Integer()
    continent_id = Integer()


class Place(Document):
    """
    Describes a place in the gazetteer index in Elasticsearch.

    Source: Who's On First https://github.com/whosonfirst-data/
    """
    name = Text(required=True)
    names = Text(required=True, multi=True)
    names_lang = Text(required=True, multi=True, fields=LANG_FIELDS)

    #TODO: figure out how to store the whole hierarchy for faster queries
    belongsto = Integer(multi=True)
    hierarchy = Nested(Hierarchy)
    parent_id = Integer()

    tags = Text(multi=True)

    # Geo-fields.
    # `location` and `geometry` are the primary fields,
    # while `geomhash` and `bbox` are auxiliary (in the
    # essence one can be used to figure out the other, but
    # included for faster queries).
    location = GeoPoint(required=True)
    geometry = GeoShape(required=True)
    geomhash = Keyword()
    bbox = Float(multi=True)

    last_updated = Date()

    timezone = Keyword()
    iso_country = Keyword()
    country = Text()

    placetype = Keyword(required=True)
    area = Float()
    area_square_m = Float()
    population = Integer(required=False)

    # Whosonfirst github URL and sha for checking if a
    # record should be updated (not required because we do not
    # want to limit the gazetteer by Whosonfirst)
    source_url = Keyword(required=False)
    github_sha = Keyword(required=False)

    # WARNING: legacy (delete after re-factoring!)
    #
    # Integration with GADM https://gadm.org/data.html
    # (6 levels)
    gadm_id_1 = Integer(required=False, multi=True)
    gadm_region_1 = Text(required=False)

    class Index:
        name = settings.ES_INDEX_LOC
        settings = {
            "number_of_shards": settings.ES_SHARDS,
            "number_of_replicas": settings.ES_REPLICAS
        }

    def __init__(self, meta=None, **kwargs):
        """
        :param meta: None or <dict> - if it contains 'remap'
            <bool:True> key, new **kwargs will be produced for
            super().__init__()
        """
        try:
            remap = meta.pop('remap', False)
        except AttributeError:
            pass
        else:
            if remap:
                source = self.get_source(**kwargs)
                meta.update(id=source.id)
                kwargs = self.prepare(source)

        super().__init__(meta, **kwargs)

    def add_hierarchy(self, **ids):
        self.hierarchy.append(Hierarchy(**ids))

    def save(self, **kwargs):
        self.last_updated = datetime.datetime.now()
        return super().save(**kwargs)

    @objectify
    def get_source(self, **kwargs):
        """Returns kwargs in a <dict> form to wrap them into RecordDict."""
        return kwargs

    def prepare(self, source):
        location = self.extract_location(source)
        if not location:
            raise MissingDataError("Could not find latitude and longitude!")

        obj = {}
        for src, trg in FIELD_MAP.items():
            try:
                obj[trg] = source.properties[src]
            except KeyError:
                obj[trg] = None

        timezone = self.extract_timezone(source)
        obj.update(self.extract_names(source))
        obj.update({
            "location": location,
            "timezone": timezone,
            "population": self.extract_population(source),
            "geometry": self.extract_geometry(source),
            "bbox": self.extract_bbox(source)
            })

        try:
            country_name_required = (obj["country"] == obj["iso_country"])
        except KeyError:
            country_name_required = True
        finally:
            if country_name_required:
                try:
                    obj.update({"country": country_name(obj["iso_country"])})
                except Exception:
                    obj.update({"country": obj["iso_country"]})

        return obj

    def extract_timezone(self, source):
        for field, val in source.properties.items():
            # Fill in language specific container
            try:
                _, tz_fieldname = field.split(":")
            except Exception:
                continue

            if tz_fieldname.lower() == "timezone":
                return val

    def extract_names(self, source):
        """Preserve lang-specific names, but pack it in a separate field."""
        names = []
        names_lang = {}
        for field, val in source.properties.items():
            if not isinstance(val, list):
                val = [val]

            # General names.
            if field.endswith(":name"):
                names.extend(val)

            # Lang-specific names.
            if field.startswith("name:") and (
                    field.endswith("_preferred") or
                    field.endswith("_variant")
                ):

                # Fill in general container (for search).
                names.extend(val)

                # Fill in language specific container
                try:
                    _, lang = field.split(":")
                    lang = lang.split("_")[0]
                except Exception:
                    continue

                else:
                    # If target language cannot be found here,
                    # fieldname is created from the first two
                    # symbols ("kor" -> "ko").
                    try:
                        lang = LANG_MAP[lang]
                    except KeyError:
                        lang = lang[:2]

                # NB: val is a list.
                try:
                    names_lang[lang].extend(val)
                except KeyError:
                    names_lang[lang] = val
                except Exception:
                    continue

        # Names can be spelled similarly in different langs.
        names = distinct_elements(flatten_list(names))
        return {
            "names": names,
            "names_lang": names_lang
            }

    def extract_population(self, source):
        try:
            return int(source.properties["wof:population"])
        except (KeyError, ValueError):
            pass

        for key, val in source.properties.items():
            if key.endswith(":population"):
                try:
                    return int(val)
                except ValueError:
                    continue

        return None

    def extract_location(self, source):
        """
        Tries to eatrct location from the `source` provided.

        :param source: <RecordDict>
        :return: <dict> (empty if nothing found)
        """
        try:
            return {
                "lat": source.properties["geom:latitude"],
                "lon": source.properties["geom:longitude"],
                }
        except KeyError:
            pass

        # Try other schemes, but always in pairs.
        lat_key, lon_key = None, None
        for key in source.properties.keys():
            if key.endswith(":latitude"):
                lat = source.properties[key]
                schema, _ = key.split(":")
                try:
                    lon = source.properties[":".join([schema, "longitude"])]
                except KeyError:
                    continue
                else:
                    return {"lat": lat, "lon": lon}

        # Last attempt...
        try:
            if source.geometry.type == 'Point':
                return {
                    "lon": source.geometry.coordinates[0],
                    "lat": source.geometry.coordinates[1]
                    }
        except (IndexError, AttributeError, KeyError):
            pass

        return {}

    def extract_geometry(self, source):
        geometry = {}
        try:
            geometry["coordinates"] = source.geometry.coordinates
        except (AttributeError, KeyError):
            # Don't need geometry if there are no coords.
            return geometry

        try:
            geometry["type"] = source.geometry.type.lower()
        except (KeyError, AttributeError):
            geometry["type"] = DEFAULT_GEOMETRY_TYPE

        return geometry

    def extract_bbox(self, source):
        try:
            return source.bbox
        except (KeyError, AttributeError):
            pass

        try:
            bbox_str = source.properties["geom:bbox"]
        except Exception:
            return []
        else:
            try:
                return [float(x) for x in bbox_str.split(",")]
            except Exception:
                return []


def setup():
    """
    Create the index template in elasticsearch specifying the mappings and any
    settings to be used. This can be run at any time, ideally at every new code
    deploy.
    """
    try:
        connections.get_connection(settings.ES_ALIAS)
    except KeyError:
        connections.add_connection(alias=settings.ES_ALIAS,
                                   conn=settings.ES_CLIENT)
    if not Place._index.exists():
        Place.init()
    if not GADM._index.exists():
        GADM.init()
