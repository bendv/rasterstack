from setuptools import setup, Extension
import os
from Cython.Build import cythonize
import numpy as np

# use a compiler with openmp support
# I had problems on mac with clang
# But gcc-9 installed with homebrew worked
# Comment/uncomment/change this line as needed
os.environ['CC'] = 'gcc-9'


ext_modules = [
    Extension(
        "rasterstack.theilsen",
        ["rasterstack/theilsen.pyx"],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp']
        )
]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

exec(read('rasterstack/__version__.py'))

setup(
    name = 'rasterstack',
    version = __version__,
    packages = ['rasterstack',],
    license = 'MIT',
    ext_modules = cythonize(ext_modules, language_level = 3),
    include_dirs = [np.get_include()],
    long_description = read('README.md'),
    install_requires = [
        'rasterio', 
        'numpy', 
        'datetime', 
        'pandas',
        'joblib',
        'cython'
        ],
    author = 'Ben DeVries',
    author_email = 'bdv@uoguelph.ca',
    url = 'http://github.com/bendv/rasterstack'
)
