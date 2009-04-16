#!/usr/bin/env python

from distutils.core import setup

setup(name='bsgit',
      version='0',
      description='Import packages from the build service into git',
      author='Andreas Gruenbacher',
      author_email='agruen@suse.de',
      license='GPLv3',

      packages=['bsgit'],
      scripts=['bsgit.py']
     )
