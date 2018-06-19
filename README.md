# About solrq

A very lightweight python package to query Apache Solr indexes. The package supports
simple queries such as:


    http://example.company.com:8983/solr/collection_1/select?q=field_1:value_1 AND field_2:value_2
    &wt=json&indent=true

and also stats such as -::


    http://example.company.com:8983/solr/collection_1/select?
    q=field_1:value_1 AND field_2:value_2 AND field_3:value_3
    &stats=true
    &stats.calcdistinct=true
    &stats.field=floor_margin_s
    &rows=0
    &wt=json&indent=false


It also supports a more complex stats query such as:


    http://example.company.com:8983/solr/collection_1/select?
    q=field_1:value_1 AND field_2:value_2 AND field_3:value_3
    &stats=true
    &stats.calcdistinct=true
    &stats.field={!min=true max=true countDistinct=true}field_2
    &stats.field={!min=true max=true countDistinct=true}field_3
    &rows=0
    &wt=json&indent=false

But you don't need to worry about setting up all these in your code and remember
the syntax that Apache Solr supports. With solrq installed,
all you need to do is to let the package know about your query and the fields
you want in your result, and the package will take care of the details for you.

## How to install the package

Download the package and cd to the installation directory and run:
```
python setup.py install
```

## How to use the package

```python
  from solrq.solr import SolrClient

  client = SolrClient("http://example.company.com:8983/solr/")

  collection = client.get_collection("collection_1")

  query = "field_1:value_1 AND field_2:value_2"

  fields = "field_1,field_2,field_3"

  result = collection.fetch(query, fields)

  fields = "field_1,field_2"

  stats = collection.stats(query, fields)
```
