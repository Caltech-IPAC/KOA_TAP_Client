from setuptools import setup, find_packages

extensions = []

reqs = ['astroquery', 'requests']

setup(
    name="koapy",
    version="1.0.0",
    author="Mihseh Kong",
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Astronomy'],
    packages=find_packages(),
    data_files=[],
    install_requires=reqs,
    python_requires='>= 3.6',
    include_package_data=False
)
