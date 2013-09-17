import os
import sys
import transaction
import logging
import gc

from sqlalchemy import engine_from_config

from pyramid.paster import (
    get_appsettings,
    setup_logging,
    )

from thesis.models import *

from thesis.scripts.seed_db import seed_db, LAYER_NAMES
from thesis.scripts.initialize_db import initialize_db

from time import sleep

def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri>\n'
          '(example: "%s development.ini")' % (cmd, cmd))
    sys.exit(1)


def get_subclasses(c):
    subclasses = c.__subclasses__()
    for d in list(subclasses):
        subclasses.extend(get_subclasses(d))

    return subclasses

def get_db_size(engine):
    res = DBSession.execute(
        "SELECT pg_database_size('%s')" % engine.url.database
    ).fetchone()
    return int(res[0])

def main(argv=sys.argv):
    if len(argv) != 2:
        usage(argv)
    config_uri = argv[1]
    setup_logging(config_uri)
    settings = get_appsettings(config_uri)
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)


    # Step 1

    all_mappable_point_classes = [MappablePoint]
    all_mappable_point_classes += get_subclasses(MappablePoint)

    bboxes = [
        [-180, -90, 180, 90], # worldwide
        [113, -43, 153, -10], # Australia
        [137.834, -10.7, 153.513336, -28.17], # Queensland
    ]

    for layer_name in LAYER_NAMES:
        log = logging.getLogger(__name__)

        log.debug("Start tests for layer: %s", layer_name)

        for class_ in all_mappable_point_classes:

            log.debug("Start for class: %s", class_.__name__)

            # Drop All
            log.debug("Dropping DB")

            DBSession.remove()
            Base.metadata.drop_all(engine)

            # Init DB
            log.debug("Init DB")
            DBSession.configure(bind=engine)
            initialize_db(engine)
            log.debug("End Init DB")

            before_db_size = None
            after_db_size = None

            # Seed DB
            log.debug("Start Seed DB")
            with transaction.manager:
                # Seed DB
                seed_db([layer_name])
            log.debug("End Seed DB")

            # Pre-Process DB
            log.debug("Start Pre-Process DB")
            before_db_size = get_db_size(engine)
            with transaction.manager:
                layer = DBSession.query(Layer).filter_by(name=layer_name).one()

                print "\n"
                # Run Pre Process
                class_.test_pre_process(layer)
                print "\n"

            # Collect Garbage
            gc.collect()

            after_db_size = get_db_size(engine)
            delta_db_size = after_db_size - before_db_size
            log.info("(%s) Start, End, Delta DB size: %i B, %i B, %i B", layer_name, before_db_size, after_db_size, delta_db_size)
            log.debug("End Pre-Process DB")

            log.debug("Start Run Tests")
            # Run Tests on DB
            with transaction.manager:
                layer = DBSession.query(Layer).filter_by(name=layer_name).one()

                for bbox in bboxes:

                    # Get geo_json for layer
                    class_.test_get_points_as_geojson(layer, bbox=bbox)

                    # Get wkt for layer
                    class_.test_get_points_as_wkt(layer, bbox=bbox)

                    # Generate clusters GeoJSON
                    class_.test_generate_clusters_geojson(layer, bbox=bbox)

                    # Generate clusters WKT
                    class_.test_generate_clusters_wkt(layer, bbox=bbox)

                    print "\n"
            log.debug("End Run Tests")

            # Collect Garbage
            gc.collect()

            log.debug("End tests for class: %s", class_.__name__)

        log.debug("End tests for layer: %s", layer_name)

        # Collect Garbage
        gc.collect()
