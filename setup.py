from setuptools import setup
import os

__version__ = '0.0.1'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = 'rasterstack',
    version = __version__,
    packages = ['rasterstack',],
    license = 'MIT',
    long_description = read('README.md'),
    install_requires = [
        'rasterio', 
        'numpy', 
        'datetime', 
        'pandas',
        'joblib'
        ],
    author = 'Ben DeVries',
    author_email = 'bdv@umd.edu',
    url = 'http://bitbucket.org/bendv/rasterstack'
)
