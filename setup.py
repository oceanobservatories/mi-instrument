#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

version = '0.3.28'

setup(name='mi-instrument',
      version=version,
      description='OOINet Marine Integrations',
      url='https://github.com/oceanobservatories/mi-instrument',
      license='BSD',
      author='Ocean Observatories Initiative',
      author_email='contactooici@oceanobservatories.org',
      keywords=['ooici'],
      packages=find_packages(),
      package_data={
          '': ['*.yml']
      },
      dependency_links=[
      ],
      test_suite='pyon',
      entry_points={
          'console_scripts': [
              'run_driver=mi.core.instrument.wrapper:main',
              'playback=mi.core.instrument.playback:main',
          ],
      },
      install_requires=[
          'ntplib>=0.1.9',
          'apscheduler==2.1.0',
          'consulate',
          'qpid-python',
          'pyzmq',
          'docopt',
          'pyyaml',
          'ooi_port_agent',
          'pika',
          'qpid-python',
          'psycopg2',
      ],
      )
