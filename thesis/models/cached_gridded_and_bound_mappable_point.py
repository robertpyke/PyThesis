from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION
import logging

from thesis.models.mappable_point import *
from thesis.models.gridded_and_bound_mappable_point import *

from sqlalchemy import (
    Column,
    Integer,
    Float,
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

class CachedGriddedAndBoundMappablePoint(GriddedAndBoundMappablePoint):

    class CacheRecord(Base):
        __tablename__ = 'cache_records'
        id = Column(Integer, primary_key=True)

        # The cache record MUST be associated with a layer
        layer_id = Column(Integer, ForeignKey("layers.id"), nullable=False)
        layer = relationship("Layer", backref=backref("cache_records", order_by=id, enable_typechecks=False))
        grid_size = Column(Float, nullable=False)

        def __init__(self, grid_size):
            """ Cache Record constructor

                Takes the following params:
                    * grid_size: The grid size of this cache
            """
            self.grid_size = grid_size

    class CachedMappablePointCluster(Base):
        __tablename__ = 'cached_mappable_point_cluster'
        id = Column(Integer, primary_key=True)

        # The chached mappable cluster MUST be associated with a cache record
        cache_record_id = Column(Integer, ForeignKey("cache_records.id"), nullable=False)
        cache_record = relationship("CacheRecord", backref=backref("cached_mappable_point_clusters", order_by=id, enable_typechecks=False))

        cluster_size = Column(Integer, nullable=False)
        centroid = Column(Geometry(geometry_type='POINT', srid=DEFAULT_PROJECTION))
        # locations = Column(Geometry(geometry_type='MULTIPOINT', srid=DEFAULT_PROJECTION))
        # cluster_envelope = Column(Geometry(geometry_type='GEOMETRY', srid=DEFAULT_PROJECTION))

        def __init__(self, cluster_size, centroid, projection=DEFAULT_PROJECTION): #locations, projection=DEFAULT_PROJECTION):
            """ CachedMappablePointCluster constructor

                Takes the following params:
                    * cluster_size: An Integer
                    * centroid: A WKT description of the cluster's centroid
                    * locations: A WKT description of the cluster's locations
                    * projection : The EPSG projection as an integer
            """
            self.cluster_size = cluster_size
            self.centroid = WKTElement(centroid, srid=projection)
#            self.locations = WKTElement(locations, srid=projection)

    @classmethod
    def pre_process(class_, layer, **kwargs):
        """ Generate the cache for all normalised grid sizes"""
        class_.generate_cache_for_all_grid_size(layer)

    @classmethod
    def generate_cache_for_all_grid_size(class_, layer):
        """ Generate the cache for all species, at all grid levels, that have out-of-date caches
            and have a total of equal to or more than cache_occurrence_clusters_threshold records.
        """
        log = logging.getLogger(__name__)
        log.debug("Generating cache for all grid sizes")

        for grid_size in class_.GRID_SIZES:
            class_.generate_cache_clusters(layer, grid_size)
            DBSession.flush()

        log.debug("Finished generating cache for all grid sizes")

    @classmethod
    def generate_cache_clusters(class_, layer, grid_size):
        log = logging.getLogger(__name__)
        log.debug("Generating cache for grid size: %s", grid_size)

        cache_record = class_.CacheRecord(grid_size)
        layer.cache_records.append(cache_record)
        DBSession.flush()

        clusters = GriddedAndBoundMappablePoint.get_points_as_wkt(layer, grid_size=grid_size)\
                .filter(
                    MappablePoint.layer_id == layer.id
                )

        i = 0
        for cluster in clusters:
            i += 1
            centroid = cluster.centroid
            cluster_size = cluster.cluster_size
#            locations = cluster.locations
            cached_mappable_cluster = class_.CachedMappablePointCluster(cluster_size, centroid) #, locations)
            cache_record.cached_mappable_point_clusters.append(cached_mappable_cluster)
            if (i % 10000 == 0):
                log.debug("Up to cluster: %i", i)
                DBSession.flush()


    @classmethod
    def get_points_as_geojson(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):

        # If no grid size was provided, calculate it from the bbox
        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        # Normalise our grid size to one of the standard grid sizes
        normalised_grid_size = class_.normalise_grid_size(grid_size)

        cache_record = next(cache_record for cache_record in layer.cache_records if cache_record.grid_size == normalised_grid_size)

        q = DBSession.query(
#            geo_func.ST_AsGeoJSON(
#                class_.CachedMappablePointCluster.locations
#            ).label("locations"),
            geo_func.ST_AsGeoJSON(
                class_.CachedMappablePointCluster.centroid
            ).label("centroid"),
            class_.CachedMappablePointCluster.cluster_size.label("cluster_size")
        ).filter(
            class_.CachedMappablePointCluster.centroid.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            class_.CachedMappablePointCluster.cache_record_id == cache_record.id
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, layer, bbox=[-180,-90,180,90], grid_size=None, **kwargs):

        # If no grid size was provided, calculate it from the bbox
        if grid_size == None:
            grid_size = class_.get_cluster_grid_size(bbox)

        # Normalise our grid size to one of the standard grid sizes
        normalised_grid_size = class_.normalise_grid_size(grid_size)

        cache_record = next(cache_record for cache_record in layer.cache_records if cache_record.grid_size == normalised_grid_size)

        q = DBSession.query(
#            geo_func.ST_AsText(
#                class_.CachedMappablePointCluster.locations
#            ).label("locations"),
            geo_func.ST_AsText(
                class_.CachedMappablePointCluster.centroid
            ).label("centroid"),
            class_.CachedMappablePointCluster.cluster_size.label("cluster_size")
        ).filter(
            class_.CachedMappablePointCluster.centroid.intersects(ST_MakeEnvelope(*bbox))
        ).filter(
            class_.CachedMappablePointCluster.cache_record_id == cache_record.id
        )

        return q

    @classmethod
    def extra_log_details(class_):
        return {
            "grid_sizes": class_.GRID_SIZES,
            "grid_sizes_length": len(class_.GRID_SIZES),
        }
