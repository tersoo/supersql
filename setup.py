# -*- coding: utf-8 -*-
from os import path, walk
from re import search
from setuptools import setup


LIBRARY = 'supersql'


package_data = {'': ['*']}
entry_points = {'console_scripts': ['supersql = supersql:main']}


def get_version(package):
    """Return package version as listed in `__version__` in `init.py`"""
    with open(path.join(package, "__init__.py")) as f:
        return search("__version__ = ['\"]([^'\"]+)['\"]", f.read()).group(1)


def get_long_description():
    with open("README.md", encoding="utf8") as f:
        return f.read()


def get_packages(package: str):
    return [
        dirpath
        for dirpath, dirnames, filenames in walk(package)
        if path.exists(path.join(dirpath, "__init__.py"))
    ]


setup_kwargs = {
    'name': f'{LIBRARY}',
    'version': get_version(LIBRARY),
    'description': 'Thin wrapper on top of SQL that enables you write SQL code in python easily',
    'long_description': get_long_description(),
    'long_description_content_type': 'text/markdown',
    'author': 'Raymond Ortserga',
    'author_email': 'codesage@live.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': f'https://github.com/rayattack/{LIBRARY}',
    'packages': get_packages(LIBRARY),
    'package_data': package_data,
    'entry_points': entry_points,
    'python_requires': '>=3.6,<4.0',
    'extras_requires': {
        "postgres": ["asyncpg"],
        "asyncpg": ["asyncpg"],
        "mysql": ["aiomysql"],
        "sqlite": ["aiosqlite"],
        "aiosqlite": ["aiosqlite"],
    }
}


setup(**setup_kwargs)
