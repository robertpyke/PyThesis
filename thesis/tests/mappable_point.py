import unittest
import transaction
import os

import csv

from pyramid import testing

from thesis.models import DBSession
from sqlalchemy import create_engine

from thesis.models import (
    Base,
    MappablePoint,
    Layer
)

class TestMappableItem(unittest.TestCase):

    def setUp(self):

        self.config = testing.setUp()
        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)
        Base.metadata.create_all(engine)

        with transaction.manager:
            # Add TestLayer1
            test_layer_1 = Layer(name='TestLayer1')

            test_layer_1.mappable_points = [
                MappablePoint('Point(30 10)'),
                MappablePoint('Point(20 10)'),
            ]

            DBSession.add(test_layer_1)

            # Add TestLayer2
            test_layer_2 = Layer(name='TestLayer2')

            test_layer_2.mappable_points = [
                MappablePoint('Point(10 15)'),
                MappablePoint('Point(10 15)'),
                MappablePoint('Point(30 15)'),
            ]

            DBSession.add(test_layer_2)

            # Add Emu Layer

            tests_path = os.path.dirname(os.path.abspath(__file__))
            test_fixtures_path = os.path.join(tests_path, 'fixtures')
            emu_csv_path = os.path.join(test_fixtures_path, 'emu.csv')

            emu_layer = Layer(name='Emu')

            with open(emu_csv_path, 'rb') as csvfile:
                emu_reader = csv.reader(csvfile)
                rownum = 0
                header = None
                for row in emu_reader:
                    # Save header row.
                    if rownum == 0:
                        header = row
                    else:
                        colnum = 0

                        latitude = 0
                        longitude = 0

                        for col in row:
                            column_label = header[colnum]

                            if column_label == "LNGDEC":
                                longitude = col
                            elif column_label == "LATDEC":
                                latitude = col

                            # print '%-8s: %s' % (column_label, col)
                            colnum += 1

                        if longitude and latitude:
                            mappable_point = MappablePoint('Point(%s %s)' % (longitude, latitude))
                            emu_layer.mappable_points.append(mappable_point)

                    rownum += 1

            DBSession.add(emu_layer)

    def tearDown(self):

        DBSession.remove()
        testing.tearDown()

        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)

        # Drop all the models
        Base.metadata.drop_all(engine)

    def test_search_layers_by_name(self):
        test_layer_1 = DBSession.query(Layer).\
            filter_by(name='TestLayer1').one()
        self.assertEqual(test_layer_1.name, 'TestLayer1')
        self.assertEqual(len(test_layer_1.mappable_points), 2)

        test_layer_2 = DBSession.query(Layer).\
            filter_by(name='TestLayer2').one()
        self.assertEqual(test_layer_2.name, 'TestLayer2')
        self.assertEqual(len(test_layer_2.mappable_points), 3)

    def test_emu_fixure_loaded(self):
        test_emu_layer = DBSession.query(Layer).\
            filter_by(name='Emu').one()
        self.assertGreater(len(test_emu_layer.mappable_points), 5)

    def test_get_layer_points_as_geo_json(self):
        q = MappablePoint.get_points_as_geojson().\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.one()
        self.assertEqual(result.locations, '{"type":"MultiPoint","coordinates":[[30,10],[20,10]]}')

        q2 = MappablePoint.get_points_as_geojson().\
            join('layer').filter(Layer.name == 'TestLayer2')
        result2 = q2.one()
        self.assertEqual(result2.locations, '{"type":"MultiPoint","coordinates":[[10,15],[10,15],[30,15]]}')

    def test_get_layer_points_as_wkt(self):
        q = MappablePoint.get_points_as_wkt().\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.one()
        self.assertEqual(result.locations, 'MULTIPOINT(30 10,20 10)')

# SELECT ST_AsGeoJSON(location) from mappable_points WHERE location && ST_MakeEnvelope(-20,-20,20,20);

# Each individual point as GeoJSON
# SELECT ST_AsGeoJSON(location) from mappable_points;

# GeoJSON of clusters snapped to grid within an envelope
# SELECT ST_AsGeoJSON(ST_COLLECT(location)) from mappable_points WHERE location && ST_MakeEnvelope(-20,-20,20,20) GROUP BY ST_SNAPTOGRID(location, 1);

# Centroid of clusters snapped to grid
# SELECT ST_AsText(ST_Centroid(ST_COLLECT(location))) from mappable_points GROUP BY ST_SNAPTOGRID(location, 1);

# All points as one collection as GeoJSON
# SELECT ST_AsGeoJSON(ST_Collect(location)) from mappable_points;
