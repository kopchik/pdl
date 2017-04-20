#!/usr/bin/env python3

from setuptools import setup
from pdl import VERSION
setup(name='pdl',
      version=VERSION,
      author="Kandalintsev Alexandre",
      author_email='spam@messir.net',
      license="Beerware",
      description="Watch your videos while downloading them in multiple streams."
                  "Chunks are downloaded in the order required to play.",
      scripts=['pdl.py']

)

