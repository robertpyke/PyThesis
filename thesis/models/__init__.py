from sqlalchemy import (
    Column,
    Integer,
    Text,
    )

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()

DEFAULT_PROJECTION = 4326

from thesis.models.layer import *
from thesis.models.mappable_point import *
from thesis.models.gridded_mappable_point import *
from thesis.models.gridded_and_bound_mappable_point import *
from thesis.models.cached_gridded_and_bound_mappable_point import *
