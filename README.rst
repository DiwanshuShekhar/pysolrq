**About the package**

A very lightweight python package to query Apache Solr indexes. The package supports
simple queries such as -

http://example.company.com:8983/solr/collection_1/select?
q=field_1:value_1 AND field_2:value_2&wt=json&indent=true

and also stats such as -

http://example.company.com:8983/solr/collection_1/select?
q=field_1:value_1 AND field_2:value_2 AND field_3:value_3
&stats=true
&stats.calcdistinct=true
&stats.field=floor_margin_s
&rows=0
&wt=json&indent=false

It also supports a more complex stats query such as -

http://example.company.com:8983/solr/collection_1/select?
q=field_1:value_1 AND field_2:value_2 AND field_3:value_3
&stats=true
&stats.calcdistinct=true
&stats.field={!min=true max=true countDistinct=true}field_2
&stats.field={!min=true max=true countDistinct=true}field_3
&rows=0
&wt=json&indent=false

**How to install the package**

1. Download the package and cd to the installation directory
2. python setup.py install

**How to use the package**

client = SolrClient("http://example.company.com:8983/solr/")

collection = client.get_collection("collection_1")

query = "field_1:value_1 AND field_2:value_2"

fields = "field_1,field_2,field_3"

result = collection.fetch(query, fields)

fields = "field_1,field_2"

stats = collection.stats(query, fields)