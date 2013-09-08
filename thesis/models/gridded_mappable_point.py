from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION

from thesis.models.mappable_point import *

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

class GriddedMappablePoint(MappablePoint):

    @classmethod
    def pre_process(class_):
        pass

    @classmethod
    def get_points_as_geojson(class_, grid_size=1):
        MappablePoint = class_

        q = DBSession.query(
            geo_func.ST_AsGeoJSON(
                ST_Collect(MappablePoint.location)
            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                geo_func.ST_Centroid(ST_Collect(MappablePoint.location))
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            ST_SnapToGrid(MappablePoint.location, grid_size)
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, grid_size=1):
        MappablePoint = class_

        q = DBSession.query(
            geo_func.ST_AsText(
                ST_Collect(MappablePoint.location)
            ).label("locations"),
            geo_func.ST_AsText(
                geo_func.ST_Centroid(ST_Collect(MappablePoint.location))
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            ST_SnapToGrid(MappablePoint.location, grid_size)
        )

        return q
