include:
  - jcu.python.python_2_7
  - jcu.postgis

virtualenv source:
  cmd.run:
    - name: wget https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.1.tar.gz --no-check-certificate
    - cwd: /tmp/
    - unless: "[ -d '/vagrant/env' ]"
    - require:
      - cmd: python_2_7 source

virtualenv decompress:
  cmd.wait:
    - name: tar -xf virtualenv-1.9.1.tar.gz
    - cwd: /tmp/
    - watch:
      - cmd: virtualenv source

create virtualenv:
  cmd.wait:
    - name: /usr/local/bin/python2.7 virtualenv.py /vagrant/env
    - cwd: /tmp/virtualenv-1.9.1
    - watch:
      - cmd: virtualenv decompress

update pip:
  cmd.wait:
    - name: ./env/bin/pip install setuptools --upgrade
    - cwd: /vagrant
    - watch:
      - cmd: create virtualenv

bootstrap:
  cmd.wait:
    - name: ./env/bin/python2.7 bootstrap.sh
    - cwd: /vagrant
    - watch:
      - cmd: update pip

buildout:
  cmd.wait:
    - name: ./bin/buildout
    - cwd: /vagrant
    - watch:
      - cmd: bootstrap
