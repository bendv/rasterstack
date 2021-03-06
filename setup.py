from setuptools import setup
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

exec(read('rasterstack/__version__.py'))

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
    author_email = 'bdv@uoguelph.ca',
    url = 'http://github.com/bendv/rasterstack'
)
