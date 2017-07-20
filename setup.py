from distutils.core import setup

with open('README.rst') as fh:
    long_description = fh.read()


setup(name='solrq',
      version='1.0.1',
      author='Diwanshu Shekhar',
      author_email='diwanshu@gmail.com',
      url='https://github.com/DiwanshuShekhar/solrq',
      description ='Extremely lightweight package to query Apache Solr indexes',
      long_description=long_description,
      requires=['requests'],
      packages=['solrq'],
      #py_modules=['solr'],
      )
