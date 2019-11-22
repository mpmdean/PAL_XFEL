from setuptools import setup

setup(name='PAL_XFEL',
      version='0.1',
      description='Compressor for PAL',
      url='http://github.com/mpmdean/h5file',
      author='Mark Dean & Yue Cao',
      author_email='mdean@bnl.gov',
      packages=['PAL_XFEL'],
      license='MIT',
      requires=['h5py', 'numpy', 'pandas', 'matplotlib'],
zip_safe=False)
