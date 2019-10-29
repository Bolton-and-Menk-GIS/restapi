from setuptools import find_packages, setup
#from distutils.core import setup
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

name = 'bmi-arcgis-restapi'

setup(name=name,
      version='1.0',
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
      install_requires=['munch', 'requests'],
      long_description=long_description,
      long_description_content_type='text/markdown'
)
