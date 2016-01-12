from distutils.core import setup

setup(name='restapi',
      version='0.1',
      description='Package for working with ArcGIS REST API',
      author='Caleb Mackey',
      author_email='calebma@bolton-menk.com',
      url='https://github.com/Bolton-and-Menk-GIS/restapi',
      license='GPL',
      package_data={'restapi': ['shapefile/*.json',
                                'test/*.py',
                                'admin/samples/*.py']},
      packages=['restapi','restapi.admin'],
      package_dir={'restapi': 'restapi'},
      zip_safe=False)


