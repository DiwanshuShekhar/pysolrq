from distutils.core import setup

with open('README.md') as fh:
    long_description = fh.read()


setup(name='pysolrq',
      version='1.0.1',
      author='Diwanshu Shekhar',
      author_email='diwanshu@gmail.com',
      url='https://github.com/DiwanshuShekhar/solrq',
      description ='Extremely lightweight package to query Apache Solr indexes',
      long_description=long_description,
      requires=['requests'],
      packages=['pysolrq'],
      #py_modules=['solr'],
      )
