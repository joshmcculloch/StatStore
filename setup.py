from setuptools import setup

setup(name='statstore',
    packages=['statstore'],
    install_requires=[
        'pymysql',
        'cryptography'
    ])
