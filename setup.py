#!/usr/bin/env python3

import string
import setuptools

def __get_version():  # noqa
    with open('bsgit.py') as code_f:
        for line in code_f:
            if line.startswith('__version__ ='):
                return line.split('=')[1].strip(string.whitespace + "'")

setuptools.setup(name='bsgit',
      version=__get_version(),
      description='Import packages from the build service into git',
      author='Andreas Gruenbacher',
      author_email='agruen@suse.de',
      maintainer='Matěj Cepl',
      maintainer_email='mcepl@cepl.eu',
      url='https://github.com/openSUSE/bsgit',
      license='GPLv3',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Build Tools',
          'Topic :: Software Development :: Version Control :: Git',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
      ],

      packages=['bsgit'],
      scripts=['bsgit.py'],
      # test_suite='tests.alltests.suite',
      # install_requires=requires_list,
     )
