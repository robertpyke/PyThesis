import logging

from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION

from thesis.models.mappable_point import *
from thesis.models.gridded_mappable_point import *

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


class ST_SnapToGrid(GenericFunction):
    """
    Return type: None
    """
    name = 'ST_SnapToGrid'
    type = None

class ST_MakeEnvelope(GenericFunction):
    """
    Return type: None
    """
    name = 'ST_MakeEnvelope'
    type = None

class BoundMappablePoint(MappablePoint):

    @classmethod
    def pre_process(class_, layer, **kwargs):
        pass

    @classmethod
    def convert_bbox_string_to_array(class_, bbox=None):
        if bbox == None:
            return None

        log = logging.getLogger(__name__)

        bbox = bbox.split(',')

        try:
            # list comprehension, convert bbox to floats
            bbox = [float(j) for j in bbox]
        except ValueError, e:
            # if we get a ValueError, it suggests the bbox arg didn't consist
            # of valid floats. Return None.
            log.warn('Invalid bbox supplied: %s. Caused Error: %s', bbox, e)
            return None;

        return bbox

    @classmethod
    def get_points_as_geojson(class_, layer, bbox=[-180,-90,180,90], **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsGeoJSON(
#                ST_Collect(MappablePoint.location)
#            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            MappablePoint.location
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            MappablePoint.layer_id == layer.id
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, layer, bbox=[-180,-90,180,90], **kwargs):
        MappablePoint = class_

        q = DBSession.query(
#            geo_func.ST_AsText(
#                ST_Collect(MappablePoint.location)
#            ).label("locations"),
            geo_func.ST_AsText(
                MappablePoint.location
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            MappablePoint.location
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            MappablePoint.layer_id == layer.id
        )

        return q
