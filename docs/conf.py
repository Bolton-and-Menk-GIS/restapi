# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('../'))
#sys.path.insert(0,'C:\Users\james.miller\restapi\restapi')
#sys.path.insert(0,'C:\Users\james.miller\restapi\restapi\admin')
#sys.path.insert(0,'C:\Users\james.miller\restapi\restapi\decorator')
#sys.path.insert(0,'C:\Users\james.miller\restapi\restapi\munch')
#sys.path.insert(0,'C:\Users\james.miller\restapi\restapi\shapefile')
#sys.path.insert(0, os.path.abspath('..'))
#sys.path.insert(0, os.path.abspath('../..'))
# -- Project information -----------------------------------------------------
import time
import shutil

project = 'restapi'
author = 'Caleb Mackey & Phil Nagel'
copyright = time.strftime('%Y, {}'.format(author))

# get version
os.environ['RESTAPI_USE_ARCPY'] = 'FALSE'
from restapi import __version__
version = __version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.githubpages',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'recommonmark',
]

napoleon_google_docstring = True
# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autodoc_mock_imports = ["munch","arcpy","shapefile"]
# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

exclude_patterns = [
    '**/decorator/*'
    '**/samples/',
    '**/shapefile/',
    '**/test/'
    'setup.py'
]

source_suffix = ['.rst', '.md']

def setup(app):
    overrides = os.path.abspath('./css/overrides.css')
    staticDir = os.path.abspath('./_build/html/_static')
    buildOverrides = os.path.join(staticDir, 'overrides.css')
    if not os.path.exists(staticDir):
        os.makedirs(staticDir)
    print('overrides: ', overrides)
    print(buildOverrides)
    shutil.copy2(overrides, buildOverrides)
    app.add_css_file('overrides.css')