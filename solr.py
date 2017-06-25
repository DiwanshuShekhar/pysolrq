import requests




class SolrClient(object):

    def __init__(self, host, version=4.7):
        self.host = host
        self.version = version

    def get_collection(self, collection, max_rows=50000):

        return SolrCollection(self.host, collection, max_rows)


class SolrCollection(SolrClient):

    def __init__(self, host, collection, max_rows=50000):
        SolrClient.__init__(self, host)
        self.collection = collection
        self.max_rows = max_rows
        self.last_call = None
        self.num_found = 0

    def pre_fetch(self, query, fields):
        """
        fetches the first 10 rows
        :param query: str
        :param fields: str
        comma separated list of fields
        :return: None
        """
        base_url = self.host + '{0}/select?'.format(self.collection)
        query_params = 'q=' + query + '&fl=' + fields + '&rows=10' + '&wt=json&indent=false'
        full_url = base_url + query_params
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        self.num_found = solr_response['response']['numFound']
    
    def fetch(self, query, fields, stats=False, ):
        """
        fetches all rows
        :param query: str
        :param fields: str
        comma separated list of fields
        :return: a list of dicts or None (if self.num_found exceeds self.max_rows)
        """
        self.pre_fetch(query, fields)

        if self.num_found > self.max_rows:
            return None

        base_url = self.host + '{0}/select?'.format(self.collection)
        query_params = 'q=' + query + '&fl=' + fields + '&rows={0}'.format(self.num_found) + '&wt=json&indent=false'
        full_url = base_url + query_params
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        documents = solr_response['response']['docs']
        return documents

    def stats(self, query, fields,
              metrics=['min', 'max', 'sum', 'count', 'missing', 'sumOfSquares'
                             'mean', 'stddev', 'percentiles', 'distinctValues', 'countDistinct',
                             'cardinality'],
              percentiles="25,50,75"):
        """
        Gets basic statistics using Solr stats
        :param query: str
        :param fields: str comma separated list of fields to compute stats on
        :param metrics: list of str list of metrics to be used
        Must be in ['min', 'max', 'sum', 'count', 'missing', 'sumOfSquares'
                    'mean', 'stddev', 'percentiles', 'distinctValues', 'countDistinct',
                    'cardinality'
                   ]
        :param percentiles str comma cut-off points to calculate percentiles
        Uses t-digest approximation algorithm
        :return: dict with metrics as keys
        """
        base_url = self.host + '{0}/select?'.format(self.collection)
        fields = fields.split(',')

        available_metrics = ['min', 'max', 'sum', 'count', 'missing', 'sumOfSquares'
                             'mean', 'stddev', 'percentiles', 'distinctValues', 'countDistinct',
                             'cardinality']

        #  selection of metrics is not supported in 4.7
        if self.version != 4.7:
            field_value = ''
            for field in fields:
                field_value = field_value + '&stats.field={!'
                for metric in metrics:
                    if metric not in available_metrics:
                        message = 'Not in: ' + str(available_metrics)
                        raise KeyError(message)
                    if metric != 'percentiles':
                        field_value = field_value + metric + "=true"

                        if metrics.index(metric) != len(metrics) - 1:
                            field_value = field_value + " "
                    else:
                        pass
                field_value = field_value + "}" + field
        else:
            field_value = ''
            for field in fields:
                field_value = field_value + '&stats.field=' + field

        #print field_value

        query_params = 'q=' + query + '&stats=true&stats.calcdistinct=true' + field_value + '&rows=0' + '&wt=json&indent=false'
        full_url = base_url + query_params
        print full_url
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        documents = solr_response['stats']['stats_fields']

        if self.version == 4.7:
            for field in fields:
                del documents[field]['distinctValues']

        return documents

    def __repr__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url

    def __str__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url

