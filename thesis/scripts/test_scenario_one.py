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
import datetime

import csv

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

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
        [151.766402, -28.141693, 153.831832, -26.855379], # Brisbane - Zoomed Out
        [152.557418, -27.738907, 153.266036, -27.271171], # Brisbance - Zoomed To City
        [152.913787,-27.679941,153.077209,-27.586867],    # Brisbane - Zommed To Suburb
        [153.051803,-27.611664,153.078582,-27.595387],    # Brisbane - Zoomed To A Few Blocks
        [153.077359,-27.597061,153.080556,-27.596623],    # Brisbane - Zoomed To A Block
    ]

    lines = []
    lines.append(["DEBUG", "layers", LAYER_NAMES])

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

                # Run Pre Process
                res_pre_process= class_.test_pre_process(layer)
                lines.append(res_pre_process)
                log.info("RES: %s", res_pre_process)

            after_db_size = get_db_size(engine)
            delta_db_size = after_db_size - before_db_size
            log.info("(%s) (%s) Start, End, Delta DB size: %i B, %i B, %i B", class_.__name__, layer_name, before_db_size, after_db_size, delta_db_size)
            log.debug("End Pre-Process DB")

            db_delta_info = ["database_delta_info", class_.__name__, layer_name, before_db_size, after_db_size, delta_db_size]
            lines.append(db_delta_info)
            log.info("RES: %s", db_delta_info)

            log.debug("Start Run Tests")
            # Run Tests on DB
            with transaction.manager:
                layer = DBSession.query(Layer).filter_by(name=layer_name).one()

                for bbox in bboxes:

                    # Get geo_json for layer
                    res_points_as_geo_json = class_.test_get_points_as_geojson(layer, bbox=bbox)

                    # Get wkt for layer
                    res_points_as_wkt = class_.test_get_points_as_wkt(layer, bbox=bbox)

                    # Generate clusters GeoJSON
                    res_points_as_geo_json_str = class_.test_get_points_as_geojson_str(layer, bbox=bbox)

                    # Generate clusters WKT
                    res_points_as_wkt_str = class_.test_get_points_as_wkt_str(layer, bbox=bbox)

                    log.info("RES: %s", res_points_as_geo_json)
                    log.info("RES: %s", res_points_as_wkt)
                    log.info("RES: %s", res_points_as_geo_json_str)
                    log.info("RES: %s", res_points_as_wkt_str)

                    lines.append(res_points_as_geo_json)
                    lines.append(res_points_as_wkt)
                    lines.append(res_points_as_geo_json_str)
                    lines.append(res_points_as_wkt_str)

            log.debug("End Run Tests")

            log.debug("End tests for class: %s", class_.__name__)

        log.debug("End tests for layer: %s", layer_name)

    time_now = datetime.datetime.now()

    results_dir = "results %s" % time_now
    mkdir_p(results_dir)

    result_file = os.path.join(results_dir, "results.csv")

    with open(result_file, 'wb') as csvfile:
        my_writer = csv.writer(csvfile, delimiter=',')
        my_writer.writerows(lines)

    MappablePoint.write_csvs(results_dir, lines)
