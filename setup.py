#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quickbooks',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Quickbooks API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbooks'],
      install_requires=[
          'singer-python==5.9.0',
          'requests==2.23.0',
          'requests_oauthlib==1.3.0',
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint==2.5.3',
              'nose'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-quickbooks=tap_quickbooks:main
      ''',
      packages=['tap_quickbooks'],
      package_data = {
          "tap_quickbooks": ["tap_quickbooks/schemas/*.json"]
      },
      include_package_data=True,
)
