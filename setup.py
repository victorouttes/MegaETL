from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='megaetl',
    packages=[
        'megaetl',
        'megaetl.hooks',
        'megaetl.operators',
        'megaetl.util',
    ],
    version='0.0.1',
    license='MIT',
    description='Airflow plugins',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Victor Outtes',
    author_email='victor.outtes@gmail.com',
    url='https://github.com/victorouttes/MegaETL',
    download_url='https://github.com/victorouttes/MegaETL/archive/refs/tags/0.0.1.tar.gz',
    keywords=['airflow', 'plugins'],
    install_requires=[
        'apache-airflow==1.10.15',
        'Cython~=0.29.23',
        'pandas~=1.2.4',
        'pyrfc~=2.4.1',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)