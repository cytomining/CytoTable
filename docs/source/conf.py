# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import pathlib
import sys

basedir = str(pathlib.Path(__file__).parent.parent.parent.resolve())

sys.path.insert(0, basedir)

# -- Project information -----------------------------------------------------

project = "CytoTable"
copyright = "2022, Cytomining Community"
author = "Cytomining Community"

# The full version, including alpha/beta/rc tags
release = "v0.0.1"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "myst_nb",
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

# turn off notebook execution for docs rendering
nb_execution_mode = "off"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []  # type: ignore


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

# set theme options
html_theme_options = {
    "font_family": "arial",
    "sidebar_width": "300px",
    "page_width": "1000px",
    "github_user": "cytomining",
    "github_repo": "CytoTable",
    "github_button": "true",
    "github_type": "star",
    "github_count": "false",
}

# set option to avoid rendering default variables
autodoc_preserve_defaults = True

# enable anchor creation
myst_heading_anchors = 3

# add custom css
html_static_path = ["_static"]
html_css_files = ["custom.css"]
