# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='libForepaas',
    version='1.2.4',
    description='Common library package for forepaas',
    long_description=readme,
    author='Jonas Vallat',
    author_email='jvallat@synotis.ch',
    url='https://github.com/JonasVallat94/libForepaas.git',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

