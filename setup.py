from setuptools import setup, find_packages

setup(name='restapi',
      version='0.1',
      description='Package for working with ArcGIS REST API',
      author='Caleb Mackey',
      author_email='calebma@bolton-menk.com',
      url='https://github.com/Bolton-and-Menk-GIS/restapi',
      license='GPL',
      packages=find_packages(),
      include_package_data=True,
##      install_requires=['requests'],
##      dependency_links=[
##          'https://pypi.python.org/pypi/requests#downloads'
##          ], #requests is now shipped with package
      zip_safe=False)


