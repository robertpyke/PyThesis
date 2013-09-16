import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

requires = [
    'pyramid',
    'SQLAlchemy',
    'GeoAlchemy2',
    'psycopg2',
    'dogpile.cache',    # cache regions, lets you cache the result of queries
    'shapely',          # PostGIS-ish operations in python
    'transaction',
    'pyramid_tm',
    'pyramid_debugtoolbar',
    'zope.sqlalchemy',
    'requests',
    'waitress',
    'pyramid_xmlrpc',
    ]

setup(name='thesis',
      version='0.0',
      description='thesis',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
      author='',
      author_email='',
      url='',
      keywords='web wsgi bfg pylons pyramid',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='thesis',
      install_requires=requires,
      entry_points="""\
      [paste.app_factory]
      main = thesis:main
      [console_scripts]
      initialize_thesis_db = thesis.scripts.initialize_db:main
      destroy_db = thesis.scripts.destroy_db:main
      seed_db = thesis.scripts.seed_db:main
      """,
      )
