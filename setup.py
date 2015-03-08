from setuptools import setup

setup(name='restapi',
      version='0.1',
      description='Package for working with ArcGIS REST API',
      author='Caleb Mackey',
      author_email='calebma@bolton-menk.com',
      url='https://github.com/Bolton-and-Menk-GIS/restapi',
      license='GPL',
      packages=['restapi'],
      install_requires=['requests'],
      include_package_data=True,
      dependency_links=[
          'https://pypi.python.org/pypi/requests#downloads'
          ],
      zip_safe=False)
