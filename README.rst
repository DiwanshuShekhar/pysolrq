A very lightweight python package to query Apache Solr indexes

How to install the package

1. Download the package and cd to the installation directory
2. python setup.py install

How to use the package

client = SolrClient("http://example.company.com:8983/solr/")
collection = client.get_collection("collection_1")

query = "field_1:value_1 AND field_2:value_2"
fields = "field_1,field_2,field_3"
result = collection.fetch(query, fields)