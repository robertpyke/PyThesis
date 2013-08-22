include:
  - jcu.python.python_2_7
  - jcu.postgis

virtualenv source:
  cmd.run:
    - name: wget https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.1.tar.gz --no-check-certificate
    - cwd: /tmp/
    - require:
      - cmd: python_2_7 make && make altinstall
    - unless: test -f /tmp/virtualenv-1.9.1.tar.gz

virtualenv decompress:
  cmd.run:
    - name: tar -xf virtualenv-1.9.1.tar.gz
    - cwd: /tmp/
    - watch:
      - cmd: virtualenv source
    - unless: test -d /tmp/virtualenv-1.9.1

create virtualenv:
  cmd.run:
    - name: /usr/local/bin/python2.7 virtualenv.py /vagrant/env
    - cwd: /tmp/virtualenv-1.9.1
    - watch:
      - cmd: virtualenv decompress
    - unless: test -d /vagrant/env

update pip:
  cmd.run:
    - name: ./env/bin/pip install setuptools --upgrade
    - cwd: /vagrant
    - watch:
      - cmd: create virtualenv

bootstrap:
  cmd.run:
    - name: ./env/bin/python2.7 bootstrap.py
    - cwd: /vagrant
    - watch:
      - cmd: update pip
    - unless: test -d /vagrant/bin

buildout:
  cmd.run:
    - name: ./bin/buildout
    - cwd: /vagrant
    - watch:
      - cmd: bootstrap
    - require:
      - cmd: Add PostgreSQL to PATH

thesis_db_user:
  postgres_user.present:
    - runas: postgres
    - password: _89_hHh_989g2988h08g2As
    - require:
      - pkg: Install PostGIS Packages
      - service: PostgreSQL Service

thesis_test_db:
  postgres_database.present:
    - runas: postgres
    - owner: thesis_db_user
    - require:
      - postgres_user: thesis_db_user

psql -d thesis_test_db -c "CREATE EXTENSION postgis;":
  cmd.wait:
    - user: postgres
    - watch:
      - pkg: Install PostGIS Packages
    - require:
      - postgres_database: thesis_test_db

thesis_dev_db:
  postgres_database.present:
    - runas: postgres
    - owner: thesis_db_user
    - require:
      - postgres_user: thesis_db_user

psql -d thesis_dev_db -c "CREATE EXTENSION postgis;":
  cmd.wait:
    - user: postgres
    - watch:
      - pkg: Install PostGIS Packages
    - require:
      - postgres_database: thesis_dev_db
