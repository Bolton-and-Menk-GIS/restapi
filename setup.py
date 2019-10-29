from distutils.core import setup
from setuptools import find_packages
import os

name = 'bmi-arcgis-restapi'
        
setup(name=name,
      version='0.1',
      description='Package for working with ArcGIS REST API',
      author='Caleb Mackey',
      author_email='calebma@bolton-menk.com',
      url='https://github.com/Bolton-and-Menk-GIS/restapi',
      license='GPL',
      packages=['restapi'],
      include_package_data=True,
      zip_Safe=False,
      package_data={'restapi': ['shapefile/*.json',
                                'test/*.py',
                                'admin/samples/*.py']},
      install_requires=['munch', 'requests']
)
