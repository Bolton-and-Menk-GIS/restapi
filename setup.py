from setuptools import setup, find_packages
import os

name = 'bmi-arcgis-restapi'

setup(name=name,
      version='0.1',
      description='Package for working with ArcGIS REST API',
      author='Caleb Mackey',
      author_email='calebma@bolton-menk.com',
      url='https://github.com/Bolton-and-Menk-GIS/restapi',
      license='GPL',
      packages=find_packages(),
      include_package_data=True,
      package_data={'restapi': ['shapefile/*.json',
                                'test/testData/*',
                                'admin/samples/*.py',
                                'projections/bin/*']},
      install_requires=['munch', 'requests']
)
