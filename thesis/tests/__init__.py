import unittest
import transaction

from pyramid import testing

from thesis.models import DBSession
from sqlalchemy import create_engine

from thesis.models import (
    Base,
    MappablePoint,
    Layer
)

class TestMyView(unittest.TestCase):

    def setUp(self):

        self.config = testing.setUp()
        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)
        Base.metadata.create_all(engine)

        with transaction.manager:
            # Add Emu
            emu_layer = Layer(name='Emu')

            emu_layer.mappable_points = [
                MappablePoint('Point(30 10)'),
                MappablePoint('Point(20 10)'),
            ]

            DBSession.add(emu_layer)

            # Add Kookaburra
            kookaburra_layer = Layer(name='Kookaburra')

            kookaburra_layer.mappable_points = [
                MappablePoint('Point(10 15)'),
                MappablePoint('Point(10 15)'),
                MappablePoint('Point(30 15)'),
            ]

            DBSession.add(kookaburra_layer)


    def tearDown(self):

        DBSession.remove()
        testing.tearDown()

        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)

        # Drop all the models
        Base.metadata.drop_all(engine)

    def test_search_layers_by_name(self):
        emu_layer = DBSession.query(Layer).\
            filter_by(name='Emu').one()
        self.assertEqual(emu_layer.name, 'Emu')
        self.assertEqual(len(emu_layer.mappable_points), 2)

        kookaburra_layer = DBSession.query(Layer).\
            filter_by(name='Kookaburra').one()
        self.assertEqual(kookaburra_layer.name, 'Kookaburra')
        self.assertEqual(len(kookaburra_layer.mappable_points), 3)
