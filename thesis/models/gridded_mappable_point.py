from thesis.models import (
    DBSession,
    Base,
    Layer,
    DEFAULT_PROJECTION,
    MappablePoint,
    BoundMappablePoint,
    ST_Collect,
    ST_SnapToGrid,
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

class GriddedMappablePoint(MappablePoint):

    """ The smallest grid size for which clustering is enabled.
        Below this value, grid size is set to None (no clustering).
    """
    MIN_GRID_SIZE_BEFORE_NO_CLUSTERING = 0.015

    """ The possible grid sizes that should be used (the normalised grid sizes) """
    GRID_SIZES = [0, MIN_GRID_SIZE_BEFORE_NO_CLUSTERING, 0.03125, 0.0625, 0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128]

    """ The grid size is the span of the window divided by GRID_SIZE_WINDOW_FRACTION
        The total number of grids will, on average, be GRID_SIZE_WINDOW_FRACTION^2
    """
    GRID_SIZE_WINDOW_FRACTION = 10

    """ The number of places to round the grid size to
    """
    ROUND_GRID_SIZE_TO_N_PLACES = 3

    @classmethod
    def normalise_grid_size(class_, grid_size):
        """ The result is normalised such that there is only a fixed number of
            possible grid sizes. This should be used for the cache interfaces to ensure
            that we don't cache to excess.
        """
        return_grid_size = None

        # Sort the grid sizes such that the smallest candidate grid sizes are first
        sorted_grid_sizes = sorted(class_.GRID_SIZES)
        for candidate_grid_size in sorted_grid_sizes:
            # If we are larger (or equal) to this grid size, normalise to it.
            if grid_size >= candidate_grid_size:
                return_grid_size = candidate_grid_size

        # This will be the last grid size we found that we were larger than (or equal to).
        return return_grid_size

    @classmethod
    def get_cluster_grid_size(class_, bbox=None):
        """ Given a bounding box, calculates an appropriate grid_size
        """

        if isinstance(bbox, basestring):
            bbox = BoundMappablePoint.convert_bbox_string_to_array(bbox)

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
    def pre_process(class_, layer, **kwargs):
        pass

    @classmethod
    def get_points_as_geojson(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):
        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        MappablePoint = class_

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
            MappablePoint.layer_id == layer.id
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):
        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        MappablePoint = class_

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
            MappablePoint.layer_id == layer.id
        )

        return q
