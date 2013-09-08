from thesis.models import DBSession, Base, Layer, DEFAULT_PROJECTION

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

from geoalchemy2 import *
from geoalchemy2.functions import GenericFunction
import geoalchemy2.functions as func


class ST_Collect(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Collect'
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
    def pre_process(class_):
        pass

    @classmethod
    def get_points_as_geojson(class_):
        MappablePoint = class_

        q = DBSession.query(
            func.ST_AsGeoJSON(
                ST_Collect(MappablePoint.location)
            ).label("locations")
        )

        return q

    @classmethod
    def get_points_as_wkt(class_):
        MappablePoint = class_

        q = DBSession.query(
            func.ST_AsText(
                ST_Collect(MappablePoint.location)
            ).label("locations")
        )

        return q
