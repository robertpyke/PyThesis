import requests

from thesis.models import DBSession, Base, DEFAULT_PROJECTION

from sqlalchemy import (
    Column,
    Integer,
    Text,
    Unicode,
    ForeignKey,
    )

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref,
    )

from geoalchemy2 import *

class Layer(Base):
    __tablename__ = 'layers'
    id = Column(Integer, primary_key=True)
    name = Column(Text)

    def __init__(self, name):
        """ Layer constructor

            Takes the following params:
                * name         : The layer's name
        """
        self.name = name

    def by_name(class_):
        """ Sort the Layers by name
        """
        Layer = class_
        q = Session.query(Layer)
        q = q.order_by(Layer.name)
        return q
