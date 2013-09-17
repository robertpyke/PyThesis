import os
import sys
import transaction
import csv
import logging

from sqlalchemy import engine_from_config

from pyramid.paster import (
    get_appsettings,
    setup_logging,
    )

from thesis.models import *

LAYER_NAMES = [
    "10_points_worldwide",
    "100_points_worldwide",
    "1k_points_worldwide",
    "10k_points_worldwide",
    "100k_points_worldwide",
    "1m_points_worldwide",
]


def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri>\n'
          '(example: "%s development.ini")' % (cmd, cmd))
    sys.exit(1)

def seed_db(layer_names = LAYER_NAMES):
    log = logging.getLogger(__name__)

    scripts_path = os.path.dirname(os.path.abspath(__file__))
    test_fixtures_path = os.path.join(scripts_path, '..', 'tests', 'fixtures')

    for layer_name in layer_names:
        layer = Layer(layer_name)
        DBSession.add(layer)

        layer_csv_path = os.path.join(test_fixtures_path, layer_name + '.csv')

        with open(layer_csv_path, 'rb') as csvfile:
            layer_reader = csv.reader(csvfile)

            i = 0
            for row in layer_reader:
                i += 1
                longitude = row[0]
                latitude = row[1]

                mappable_point = MappablePoint('Point(%s %s)' % (longitude, latitude))
                layer.mappable_points.append(mappable_point)

                if (i % 10000 == 0):
                    log.debug("Up to point: %i", i)
                    DBSession.flush()

        DBSession.flush()


def main(argv=sys.argv):
    if len(argv) != 2:
        usage(argv)
    config_uri = argv[1]
    setup_logging(config_uri)
    settings = get_appsettings(config_uri)
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    with transaction.manager:
        seed_db()
