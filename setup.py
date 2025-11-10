import os
import sys
from setuptools import setup, find_packages
from fnmatch import fnmatchcase
from distutils.util import convert_path

standard_exclude = ('*.pyc', '*~', '.*', '*.bak', '*.swp*')
standard_exclude_directories = ('.*', 'CVS', '_darcs', './build', './dist', 'EGG-INFO', '*.egg-info')
def find_package_data(where='.', package='', exclude=standard_exclude, exclude_directories=standard_exclude_directories):
    out = {}
    stack = [(convert_path(where), '', package)]
    while stack:
        where, prefix, package = stack.pop(0)
        for name in os.listdir(where):
            fn = os.path.join(where, name)
            if os.path.isdir(fn):
                bad_name = False
                for pattern in exclude_directories:
                    if (fnmatchcase(name, pattern)
                        or fn.lower() == pattern.lower()):
                        bad_name = True
                        break
                if bad_name:
                    continue
                if os.path.isfile(os.path.join(fn, '__init__.py')):
                    if not package:
                        new_package = name
                    else:
                        new_package = package + '.' + name
                        stack.append((fn, '', new_package))
                else:
                    stack.append((fn, prefix + name + '/', package))
            else:
                bad_name = False
                for pattern in exclude:
                    if (fnmatchcase(name, pattern)
                        or fn.lower() == pattern.lower()):
                        bad_name = True
                        break
                if bad_name:
                    continue
                out.setdefault(package, []).append(prefix+name)
    return out

setup(name='docassemble.MACourts',
      version='1.1.1',
      description=('List Massachusetts Courts in Docassemble'),
      long_description='A utility package that includes JSON files representing all of the courts in Massachusetts.\r\n\r\nCourts automatically scraped from Mass.gov and geocoded with Google Maps.\r\n\r\nIf the court has a PO box, the PO box will be available in court.address.orig_address',
      long_description_content_type='text/markdown',
      author='Quinten Steenhuis',
      author_email='qsteenhuis@gbls.org',
      license='The MIT License (MIT)',
      url='https://docassemble.org',
      packages=find_packages(),
      namespace_packages=['docassemble'],
      install_requires=['Shapely>=1.0.15', 'geopandas>=0.2.0', 'usaddress>=0.5.10', 'shapely>=2.0.6'],
      zip_safe=False,
      package_data=find_package_data(where='docassemble/MACourts/', package='docassemble.MACourts'),
     )

