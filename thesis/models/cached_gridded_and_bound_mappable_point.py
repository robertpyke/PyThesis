from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION
import logging
import ipdb

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
        cluster_centroid = Column(Geometry(geometry_type='POINT', srid=DEFAULT_PROJECTION))
        # cluster_envelope = Column(Geometry(geometry_type='GEOMETRY', srid=DEFAULT_PROJECTION))

        def __init__(self, cluster_size, cluster_centroid_wkt, projection=DEFAULT_PROJECTION):
            """ CachedMappablePointCluster constructor

                Takes the following params:
                    * cluster_size: An Integer
                    * cluster_centroid_wkt: A WKT description of the cluster's centroid
                    * projection : The EPSG projection as an integer
            """
            self.cluster_size = cluster_size
            self.cluster_centroid = WKTElement(cluster_centroid_wkt, srid=projection)

    @classmethod
    def pre_process(class_, **kwargs):
        """ Generate the cache for all normalised grid sizes"""
        layers = DBSession.query(Layer).all()
        for layer in layers:
            class_.generate_cache_for_all_grid_size(layer)

    @classmethod
    def generate_cache_for_all_grid_size(class_, layer):
        """ Generate the cache for all species, at all grid levels, that have out-of-date caches
            and have a total of equal to or more than cache_occurrence_clusters_threshold records.
        """
        log = logging.getLogger(__name__)
        log.info("Generating cache for all grid sizes")

        for grid_size in class_.GRID_SIZES:
            class_.generate_cache_clusters(layer, grid_size)

        log.info("Finished generating cache for all grid sizes")

    @classmethod
    def generate_cache_clusters(class_, layer, grid_size):
        log = logging.getLogger(__name__)
        log.info("Generating cache for grid size: %s", grid_size)

        cache_record = class_.CacheRecord(grid_size)

        clusters = GriddedAndBoundMappablePoint.get_points_as_wkt(grid_size=grid_size)\
                .filter(
                    MappablePoint.layer_id == layer.id
                )

        for cluster in clusters:
            centroid = cluster.centroid
            cluster_size = cluster.cluster_size
            cached_mappable_cluster = class_.CachedMappablePointCluster(cluster_size, centroid)
            cache_record.cached_mappable_point_clusters.append(cached_mappable_cluster)

        layer.cache_records.append(cache_record)

    @classmethod
    def get_points_as_geojson(class_, bbox=[-180,-90,180,90], grid_size=None):
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
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        )

        return q

    @classmethod
    def get_points_as_wkt(class_, bbox=[-180,-90,180,90], grid_size=None):
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
        ).filter(
            MappablePoint.location.intersects(ST_MakeEnvelope(*bbox))
        )

        return q
