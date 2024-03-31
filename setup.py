from setuptools import setup, find_packages
with open('ants_ai/README.rst') as f:
    readme = f.read()
with open('ants_ai/LICENSE') as f:
    module_license = f.read()

setup(
    name='1brc_turan',
    version='0.0.0',
    description='Implementation of 1 billion row challange in python',
    long_description=readme,
    author='Jason Turan',
    author_email='turan.jason@gmail.com',
    url='https://github.com/Jason-Turan0/1brc_turan',
    license=module_license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=['invoke']
)