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

class GriddedAndBoundMappablePoint(GriddedMappablePoint):

    """ The grid size is the span of the window divided by GRID_SIZE_WINDOW_FRACTION
        The total number of grids will, on average, be GRID_SIZE_WINDOW_FRACTION^2
    """
    GRID_SIZE_WINDOW_FRACTION = 10

    """ The number of places to round the grid size to
    """
    ROUND_GRID_SIZE_TO_N_PLACES = 3

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
            # of valid floats. We can't calculate grid_size, so return None.
            log.warn('Invalid bbox supplied: %s. Caused Error: %s', bbox, e)
            return None;

        return bbox

    @classmethod
    def get_cluster_grid_size(class_, bbox=None):
        """ Given a bounding box, calculates an appropriate grid_size
        """

        if isinstance(bbox, basestring):
            bbox = class_.convert_bbox_string_to_array(bbox)

        if bbox == None:
            return None

        w, s, e, n = bbox

        lat_range = abs(w - e)
        lng_range = abs(n - s)

        lat_lng_range_avg = ( (lat_range + lng_range) / 2 )

        grid_size = ( lat_lng_range_avg / float(class_.GRID_SIZE_WINDOW_FRACTION) )
        grid_size = round(grid_size, class_.ROUND_GRID_SIZE_TO_N_PLACES)

        if grid_size < class_.MIN_GRID_SIZE_BEFORE_NO_CLUSTERING:
            grid_size = 0

        return grid_size

    @classmethod
    def get_points_as_geojson(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):
        MappablePoint = class_

        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

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
            geo_func.ST_AsText(
                ST_Collect(MappablePoint.location)
            ).label("locations"),
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
