#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

version = '0.6.7'

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
          '': ['*.yml'],
          'mi.platform.rsn': ['node_config_files/*.yml'],
      },
      dependency_links=[
      ],
      test_suite='pyon',
      entry_points={
          'console_scripts': [
              'run_driver=mi.core.instrument.wrapper:main',
              'playback=mi.core.instrument.playback:main',
              'analyze=mi.core.instrument.playback_analysis:main',
              'oms_extractor=mi.platform.rsn.oms_extractor:main',
              'shovel=mi.core.shovel:main',
              'oms_aa_server=mi.platform.rsn.oms_alert_alarm_server:main',
              'zplsc_echogram=mi.dataset.driver.zplsc_c.zplsc_echogram_generator:main',
          ],
      },
      )
