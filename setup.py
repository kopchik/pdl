#!/usr/bin/env python3

from setuptools import setup
from pdl import VERSION
setup(name='pdl',
      version=VERSION,
      author="Kandalintsev Alexandre",
      author_email='spam@messir.net',
      license="Beerware",
      description="Watch you video while it downloads in multiple streams."
                  "Chunks of the file are downloaded in order required to play.",
      scripts=['pdl.py']

)

