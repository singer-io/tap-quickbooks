#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quickbooks',
      version='2.3.0',
      description='Singer.io tap for extracting data from the Quickbooks API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbooks'],
      install_requires=[
          'singer-python==6.1.1',
          'requests==2.32.3',
          'requests_oauthlib==2.0.0',
      ],
      extras_require={
          'test': [
              'pylint==2.5.3',
              'nose'
          ],
          'dev': [
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-quickbooks=tap_quickbooks:main
      ''',
      packages=['tap_quickbooks'],
      package_data = {
          "tap_quickbooks": [
              "schemas/*.json",
              "schemas/shared/*.json"
          ]
      },
      include_package_data=True,
)
