import logging

from thesis.models import (
    DBSession,
    Base,
    Layer,
    DEFAULT_PROJECTION,
    ST_Collect,
    ST_SnapToGrid,
    ST_MakeEnvelope,
    MappablePoint,
    GriddedMappablePoint,
    BoundMappablePoint,
    )

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

class GriddedAndBoundMappablePoint(GriddedMappablePoint):


    @classmethod
    def pre_process(class_, layer, **kwargs):
        pass


    @classmethod
    def get_points_as_geojson(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):
        MappablePoint = class_

        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        q = DBSession.query(
#            geo_func.ST_AsGeoJSON(
#                ST_Collect(MappablePoint.location)
#            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                geo_func.ST_Centroid(ST_Collect(MappablePoint.location))
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            ST_SnapToGrid(MappablePoint.location, grid_size)
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            MappablePoint.layer_id == layer.id
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):
        MappablePoint = class_

        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        q = DBSession.query(
#            geo_func.ST_AsText(
#                ST_Collect(MappablePoint.location)
#            ).label("locations"),
            geo_func.ST_AsText(
                geo_func.ST_Centroid(ST_Collect(MappablePoint.location))
            ).label('centroid'),
            func.count(MappablePoint.location).label('cluster_size')
        ).group_by(
            ST_SnapToGrid(MappablePoint.location, grid_size)
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            MappablePoint.layer_id == layer.id
        )

        return q
