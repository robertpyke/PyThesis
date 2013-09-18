from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION
import logging
import datetime
import json

from sqlalchemy import (
    Column,
    Integer,
    Text,
    Unicode,
    ForeignKey,
    )

from sqlalchemy.sql import expression

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref,
    )

from sqlalchemy import func as func
from geoalchemy2 import *
from geoalchemy2.functions import GenericFunction
import geoalchemy2.functions as geo_func

class ST_Collect(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Collect'
    type = types.Geometry

class ST_Multi(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Multi'
    type = types.Geometry

class MappablePoint(Base):
    __tablename__ = 'mappable_points'
    id = Column(Integer, primary_key=True)

    # The mappable point MUST be associated with a layer
    layer_id = Column(Integer, ForeignKey("layers.id"), nullable=False)
    layer = relationship("Layer", backref=backref("mappable_points", order_by=id, enable_typechecks=False))
    location = Column(Geometry(geometry_type='POINT', srid=DEFAULT_PROJECTION))

    def __init__(self, location_wkt, projection=DEFAULT_PROJECTION):
        """ Mappable Point constructor

            Takes the following params:
                * location_wkt : A WKT description of the mappable point
                * projection : The EPSG projection as an integer
        """
        self.location = WKTElement(location_wkt, srid=projection)

    @classmethod
    def pre_process(class_, layer, **kwargs):
        pass

    @classmethod
    def test_pre_process(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: pre_process")

        start_t = datetime.datetime.now()

        result = class_.pre_process(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: pre_process")

        log.info(
            "(%s) pre_process(%s, %s) took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            delta_t_s,
        )

        return ["pre_process", class_.__name__, layer.name, delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def get_points_as_geojson(class_, layer, **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsGeoJSON(
#                ST_Multi(ST_Collect(MappablePoint.location))
#            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).filter(
            MappablePoint.layer_id == layer.id
        ).group_by(
            MappablePoint.location
        )

        return q

    @classmethod
    def test_get_points_as_geojson(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_geojson")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_geojson(layer, **kwargs).all()

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_geojson")

        log.info(
            "(%s) get_points_as_geojson(%s, %s) clusters: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_geojson", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def get_points_as_geojson_str(class_, layer, **kwargs):
        q = class_.get_points_as_geojson(layer, **kwargs)
        result = q.all()

        return_geojson_obj = {
            "type": "FeatureCollection",
            "features": [
            ]
        }

        for el in result:
            centroid     = json.loads(el.centroid)
            cluster_size = el.cluster_size

            return_geojson_obj["features"].append({
                "type": "Feature",
                "geometry": centroid,
                "properties": {
                    "cluster_size": cluster_size
                }
            })

        return json.dumps(return_geojson_obj)

    @classmethod
    def test_get_points_as_geojson_str(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_geojson_str")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_geojson_str(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_geojson_str")

        log.info(
            "(%s) get_points_as_geojson_str(%s, %s) string length: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_geojson_str", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def get_points_as_wkt(class_, layer, **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsText(
#                ST_Multi(ST_Collect(MappablePoint.location))
#            ).label("locations"),
            geo_func.ST_AsText(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).filter(
            MappablePoint.layer_id == layer.id
        ).group_by(
            MappablePoint.location
        )

        return q

    @classmethod
    def test_get_points_as_wkt(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_wkt")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_wkt(layer, **kwargs).all()

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_wkt")

        log.info(
            "(%s) get_points_as_wkt(%s, %s) clusters: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_wkt", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def get_points_as_wkt_str(class_, layer, **kwargs):
        q = class_.get_points_as_wkt(layer, **kwargs)
        result = q.all()

        wkt_centroids = []

        for el in result:
            centroid_wkt     = el.centroid

            wkt_centroids.append(centroid_wkt)

        return_str = "GEOMETRYCOLLECTION(" + ','.join(wkt_centroids) + ")"
        return return_str

    @classmethod
    def test_get_points_as_wkt_str(class_, layer, **kwargs):
        log = logging.getLogger(__name__)

        log.debug("Start: get_points_as_wkt_str")

        start_t = datetime.datetime.now()

        result = class_.get_points_as_wkt_str(layer, **kwargs)

        end_t = datetime.datetime.now()
        delta_t = end_t - start_t
        delta_t_s = delta_t.seconds + ( delta_t.microseconds *  (10 ** -6) )

        log.debug("End: get_points_as_wkt_str")



        log.info(
            "(%s) get_points_as_wkt_str(%s, %s) string length: %i, took: seconds: %i",
            class_.__name__,
            layer.name,
            kwargs,
            len(result),
            delta_t_s,
        )

        return ["get_points_as_wkt_str", class_.__name__, layer.name, len(result), delta_t_s, kwargs, class_.extra_log_details()]

    @classmethod
    def extra_log_details(class_):
        return {}
