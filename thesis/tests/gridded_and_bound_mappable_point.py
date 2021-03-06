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
        test_layer_1 = DBSession.query(Layer).filter_by(name='TestLayer1').one()
        test_layer_2 = DBSession.query(Layer).filter_by(name='TestLayer2').one()

        q = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_1, grid_size=1)
        result = q.all()
#        self.assertEqual(result[0].locations, '{"type":"MultiPoint","coordinates":[[20,10]]}')
#        self.assertEqual(result[1].locations, '{"type":"MultiPoint","coordinates":[[30,10]]}')

        q2 = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_1, grid_size=100)
        result2 = q2.all()
#        self.assertEqual(result2[0].locations, '{"type":"MultiPoint","coordinates":[[30,10],[20,10]]}')
        self.assertEqual(result2[0].cluster_size, 2)

        q3 = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_2, grid_size=1)
        result3 = q3.all()
#        self.assertEqual(result3[0].locations, '{"type":"MultiPoint","coordinates":[[10,15],[10,15]]}')
#        self.assertEqual(result3[1].locations, '{"type":"MultiPoint","coordinates":[[30,15]]}')
        self.assertEqual(result3[0].cluster_size, 2)
        self.assertEqual(result3[1].cluster_size, 1)

    def test_get_cluster_centroids_as_geo_json(self):
        test_layer_1 = DBSession.query(Layer).filter_by(name='TestLayer1').one()
        test_layer_2 = DBSession.query(Layer).filter_by(name='TestLayer2').one()

        q = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_1, grid_size=1)
        result = q.all()
        self.assertEqual(result[0].centroid, '{"type":"Point","coordinates":[20,10]}')
        self.assertEqual(result[1].centroid, '{"type":"Point","coordinates":[30,10]}')
        self.assertEqual(result[0].cluster_size, 1)
        self.assertEqual(result[1].cluster_size, 1)

        q2 = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_1, grid_size=100)
        result2 = q2.one()
        self.assertEqual(result2.centroid, '{"type":"Point","coordinates":[25,10]}')

        q3 = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_2, grid_size=100)
        result3 = q3.one()
        self.assertEqual(result3.centroid, '{"type":"Point","coordinates":[16.6666666666667,15]}')

    def test_bounds_not_intersecting_points(self):
        test_layer_1 = DBSession.query(Layer).filter_by(name='TestLayer1').one()
        q = GriddedAndBoundMappablePoint.get_points_as_geojson(test_layer_1, grid_size=1, bbox=[-180,-89,-170,-80])
        result = q.all()
        self.assertEqual(len(result),0)

    def test_get_layer_points_as_wkt(self):
        test_layer_1 = DBSession.query(Layer).filter_by(name='TestLayer1').one()
        q = GriddedAndBoundMappablePoint.get_points_as_wkt(test_layer_1, grid_size=1)
        result = q.all()
#        self.assertEqual(result[0].locations, 'MULTIPOINT(20 10)')
#        self.assertEqual(result[1].locations, 'MULTIPOINT(30 10)')

    def test_get_cluster_grid_size(self):
        bounds_1 = "-180, -90, 180, 90"
        grid_size_1 = GriddedAndBoundMappablePoint.get_cluster_grid_size(bounds_1)
        self.assertEqual(grid_size_1, (270.0 / GriddedAndBoundMappablePoint.GRID_SIZE_WINDOW_FRACTION))

        bounds_2 = "0, 0, 180, 90"
        grid_size_2 = GriddedAndBoundMappablePoint.get_cluster_grid_size(bounds_2)
        self.assertEqual(grid_size_2, (135.0 / GriddedAndBoundMappablePoint.GRID_SIZE_WINDOW_FRACTION))

        bounds_3 = "0, 0, 10, 10"
        grid_size_3 = GriddedAndBoundMappablePoint.get_cluster_grid_size(bounds_3)
        self.assertEqual(grid_size_3, (10.0 / GriddedAndBoundMappablePoint.GRID_SIZE_WINDOW_FRACTION))

        bounds_4 = "0, 0, 0.0001, 0.0001"
        grid_size_4 = GriddedAndBoundMappablePoint.get_cluster_grid_size(bounds_4)
        self.assertTrue( ( grid_size_4 / GriddedAndBoundMappablePoint.GRID_SIZE_WINDOW_FRACTION ) < GriddedAndBoundMappablePoint.MIN_GRID_SIZE_BEFORE_NO_CLUSTERING)
        self.assertEqual(grid_size_4, 0)

        bounds_5 = "0, b, 0.0001, 0.0001"
        grid_size_5 = GriddedAndBoundMappablePoint.get_cluster_grid_size(bounds_5)
        self.assertEqual(None, grid_size_5)
