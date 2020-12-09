from distutils.core import setup

setup(
    name='OpenAQ-FastAPI',
    version='0.0.1',
    author='David Bitner',
    author_email='david@developmentseed.org',
    packages=['openaq_fastapi'],
    url='http://openaq.org/',
    license='LICENSE.txt',
    description='FastAPI API For OpenAQ',
    long_description=open('README.md').read(),
    install_requires=[
        'fastapi @ git+https://github.com/bitner/fastapi.git@multialias',
        'mangum>=0.1.0',
        'fastapi-utils',
        'wheel',
        'pypika',
        'asyncpg',
        'pydantic[dotenv]',
        'buildpg',
        'aiocache',
        'jq',
        'orjson',
        'uvicorn',
        'msgpack',
        'asyncpg',
        'uvicorn',
        'humps',
    ],
    extras_require={
        'dev': [
            'black',
            'flake8',
        ]
    },
    entry_points={
        'console_scripts': [
            'openaq_fastapi=openaq_fastapi.main:run'
        ]
    },
)