import unittest
import transaction
import os

import csv

from pyramid import testing

from thesis.models import DBSession
from sqlalchemy import create_engine

from thesis.models import (
    Base,
    GriddedAndBoundMappablePoint,
    Layer
)

class TestGriddedAndBoundMappableItem(unittest.TestCase):

    def setUp(self):

        self.config = testing.setUp()
        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)
        Base.metadata.create_all(engine)

        with transaction.manager:
            # Add TestLayer1
            test_layer_1 = Layer(name='TestLayer1')

            test_layer_1.mappable_points = [
                GriddedAndBoundMappablePoint('Point(30 10)'),
                GriddedAndBoundMappablePoint('Point(20 10)'),
            ]

            DBSession.add(test_layer_1)

            # Add TestLayer2
            test_layer_2 = Layer(name='TestLayer2')

            test_layer_2.mappable_points = [
                GriddedAndBoundMappablePoint('Point(10 15)'),
                GriddedAndBoundMappablePoint('Point(10 15)'),
                GriddedAndBoundMappablePoint('Point(30 15)'),
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
                            mappable_point = GriddedAndBoundMappablePoint('Point(%s %s)' % (longitude, latitude))
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
        q = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=1).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.all()
        self.assertEqual(result[0].locations, '{"type":"MultiPoint","coordinates":[[20,10]]}')
        self.assertEqual(result[1].locations, '{"type":"MultiPoint","coordinates":[[30,10]]}')

        q2 = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=100).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result2 = q2.all()
        self.assertEqual(result2[0].locations, '{"type":"MultiPoint","coordinates":[[30,10],[20,10]]}')
        self.assertEqual(result2[0].cluster_size, 2)

        q3 = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=1).\
            join('layer').filter(Layer.name == 'TestLayer2')
        result3 = q3.all()
        self.assertEqual(result3[0].locations, '{"type":"MultiPoint","coordinates":[[10,15],[10,15]]}')
        self.assertEqual(result3[1].locations, '{"type":"MultiPoint","coordinates":[[30,15]]}')
        self.assertEqual(result3[0].cluster_size, 2)
        self.assertEqual(result3[1].cluster_size, 1)

    def test_get_cluster_centroids_as_geo_json(self):
        q = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=1).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.all()
        self.assertEqual(result[0].centroid, '{"type":"Point","coordinates":[20,10]}')
        self.assertEqual(result[1].centroid, '{"type":"Point","coordinates":[30,10]}')
        self.assertEqual(result[0].cluster_size, 1)
        self.assertEqual(result[1].cluster_size, 1)

        q2 = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=100).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result2 = q2.one()
        self.assertEqual(result2.centroid, '{"type":"Point","coordinates":[25,10]}')

        q3 = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=100).\
            join('layer').filter(Layer.name == 'TestLayer2')
        result3 = q3.one()
        self.assertEqual(result3.centroid, '{"type":"Point","coordinates":[16.6666666666667,15]}')

    def test_bounds_not_intersecting_points(self):
        q = GriddedAndBoundMappablePoint.get_points_as_geojson(grid_size=1, bounds=[-180,-89,-170,-80]).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.all()
        self.assertEqual(len(result),0)

    def test_get_layer_points_as_wkt(self):
        q = GriddedAndBoundMappablePoint.get_points_as_wkt(grid_size=1).\
            join('layer').filter(Layer.name == 'TestLayer1')
        result = q.all()
        self.assertEqual(result[0].locations, 'MULTIPOINT(20 10)')
        self.assertEqual(result[1].locations, 'MULTIPOINT(30 10)')

