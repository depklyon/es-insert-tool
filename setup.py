from setuptools import setup

setup(
    name='Elasticsearch Insert Tool',
    version='1.0.0',
    packages=['import_es'],
    url='https://github.com/depklyon/es-insert-tool',
    license='MIT',
    author='Patrick Palma',
    author_email='7872736+depklyon@users.noreply.github.com',
    description='Tool to import CSV files into Elasticsearch',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
    ],
    python_requires='>=3',
    install_requires=['elasticsearch', 'PyYAML'],
    keywords='elasticsearch python csv import'
)
