#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quickbooks',
      version='2.2.0',
      description='Singer.io tap for extracting data from the Quickbooks API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbooks'],
      install_requires=[
          'singer-python==6.0.0',
          'requests==2.31.0',
          'requests_oauthlib==1.3.0',
      ],
      extras_require={
          'test': [
              'pylint==3.0.3',
              'nose2'
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
