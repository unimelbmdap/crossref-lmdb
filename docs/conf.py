project = "crossref-lmdb"
copyright = "2024, MDAP"
author = "MDAP"
version = "0.1.0"
release = version

extensions = [
    "sphinx_toolbox.more_autodoc",
    "sphinx_toolbox.github",
    "sphinx_toolbox.more_autodoc.genericalias",
    "sphinx_toolbox.more_autodoc.sourcelink",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinxarg.ext",
    "enum_tools.autoenum",
]

templates_path = ["_templates"]
exclude_patterns = []

maximum_signature_line_length = 88

html_theme = "furo"
html_static_path = ["_static"]

html_show_copyright = False
html_show_sphinx = False

html_theme_options = {}

pygments_dark_style = "github-dark"

autodoc_typehints = "both"
autodoc_member_order = "bysource"
autodoc_preserve_defaults = True
autoclass_content = "init"

typehints_use_signature = True
typehints_fully_qualified = True
typehints_always_use_bars_union = True
typehints_defaults = "braces"

github_username = "unimelbmdap"
github_repository = "crossref-lmdb"
autodoc_show_sourcelink = True


def setup(app):
    app.add_css_file("types_fix.css")
    app.add_css_file("argparse_fix.css")
