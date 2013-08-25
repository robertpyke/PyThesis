import unittest
import transaction

from pyramid import testing

from thesis.models import DBSession
from sqlalchemy import create_engine
        
from thesis.models import (
    Base,
    MyModel,
)


class TestMyView(unittest.TestCase):

    def setUp(self):

        self.config = testing.setUp()
        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)
        Base.metadata.create_all(engine)

        with transaction.manager:
            model = MyModel(name='one', value=55)
            DBSession.add(model)

    def tearDown(self):

        DBSession.remove()
        testing.tearDown()

        engine = create_engine('postgresql+psycopg2://thesis_db_user:_89_hHh_989g2988h08g2As@127.0.0.1:5432/thesis_test_db')
        DBSession.configure(bind=engine)

        # Drop all the models
        Base.metadata.drop_all(engine)

    def test_it(self):

        from thesis.views import my_view
        request = testing.DummyRequest()
        info = my_view(request)
        self.assertEqual(info['one'].name, 'one')
        self.assertEqual(info['project'], 'thesis')
