import requests
import uuid
import xmltodict


class SolrClient(object):

    def __init__(self, host, version=4.7):
        """
        Constructor for SolrClient

        :param host: string
                Solr host Example:http://example.company.com:8983/solr/
        :param version: float, default = 4.7
                Current version of Solr host
        """
        self.host = host
        self.version = version

    def get_collection(self, collection, max_rows=50000):
        """
        Generator method to return SolrCollection
        :param collection: string
                    name of Solr collection
        :param max_rows: int, default = 50000
                    maximum rows to fetch
        :return: SolrCollection object
        """

        return SolrCollection(self.host, collection, max_rows)

    def get_control(self, collection):
        """
        Generator method to return SolrCollection
        :param collection: string
                    name of Solr collection
        :return: SolrIndexer object
        """

        return SolrControl(self.host, collection)


class SolrCollection(SolrClient):
    """
    SolrCollection class
    Never have to be instantiated directly. get_collection method of
    SolrClient object instantiates SolrCollection object
    """

    def __init__(self, host, collection, max_rows=50000):
        """
        Constructor for SolrCollection
        :param host: string
                Solr host Example:http://example.company.com:8983/solr/

        :param collection: string
                name of Solr collection

        :param max_rows:
                maximum rows to fetch
        """
        SolrClient.__init__(self, host)
        self.collection = collection
        self.max_rows = max_rows
        self.last_call = None
        self.num_found = 0

    def pre_fetch(self, query, fields):
        """
        fetches the first 10 rows
        :param query: str
                Query string. Example: 'field1:val1 AND field2:val2'
        :param fields: str
                comma separated list of fields. Example: [field1, field3]

        :return: None
        """

        base_url = self.host + '{0}/select?'.format(self.collection)
        query_params = 'q=' + query + '&fl=' + fields + '&rows=10' + '&wt=json&indent=false'
        full_url = base_url + query_params
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        self.num_found = solr_response['response']['numFound']
    
    def fetch(self, query, fields=None, num_rows=None):
        """
        fetches all rows
        :param query: str
                    Query string. Example: 'field1:val1 AND field2:val2'

        :param fields: str
                    comma separated list of fields. Example: [field1, field3]

        :param num_rows: int
                        number of rows to fetch

        :return: a list of dicts or None if self.num_found exceeds self.max_rows
        """
        if fields is None:
            fields = '*'

        self.pre_fetch(query, fields)

        if num_rows is None:
            if self.num_found > self.max_rows:
                return None
            else:
                num_rows = self.num_found
        else:
            if num_rows > self.num_found:
                num_rows = self.num_found

        base_url = self.host + '{0}/select?'.format(self.collection)
        query_params = 'q=' + query + '&fl=' + fields + '&rows={0}'.format(num_rows) + '&wt=json&indent=false'
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
                    Query string. Example: 'field1:val1 AND field2:val2'

        :param fields: str
                    comma separated list of fields to compute stats on. Example: [field1, field3]

        :param metrics: list of str list of metrics to be used
                    Must be in ['min', 'max', 'sum', 'count', 'missing', 'sumOfSquares'
                    'mean', 'stddev', 'percentiles', 'distinctValues', 'countDistinct',
                    'cardinality'
                   ]

        :param percentiles:
                    string of numbers separated by commas to calculate percentiles at
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
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        documents = solr_response['stats']['stats_fields']

        if self.version == 4.7:
            for field in fields:
                del documents[field]['distinctValues']

        return documents

    def facet_range(self, query, field_params):
        """
        Get facet results using Solr Facets
        :param query: str

        :param field_params: dict
                        Example: {field_1:[start, end, gap, include], field_2:[start, end, gap, include]}

        :param bins: int

        :return: dict
        """
        base_url = self.host + '{0}/select?'.format(self.collection)

        field_value = ''
        for field in field_params.keys():
            field_value = field_value + '&facet.range=' + field
            field_value = field_value + '&f.' + field + '.facet.range.start={0}'.format(field_params[field][0])
            field_value = field_value + '&f.' + field + '.facet.range.end={0}'.format(field_params[field][1])
            field_value = field_value + '&f.' + field + '.facet.range.gap={0}'.format(field_params[field][2])
            field_value = field_value + '&f.' + field + '.facet.range.include={0}'.format(field_params[field][3])

        query_params = 'q=' + query + '&facet=true' + field_value + '&rows=0' + '&wt=json&indent=false'

        full_url = base_url + query_params
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        print solr_response
        documents = solr_response['facet_counts']['facet_ranges']
        return documents

    def __repr__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url

    def __str__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url


class SolrControl(SolrClient):

    def __init__(self, host, collection):
        """

        :param host:
        :param collection:
        """
        SolrClient.__init__(self, host)
        self.collection = collection

    def make_collection(self, num_shards):
        """
        This assumes that the user has already uploaded the configuration to zookeeper
        :param name: name of the collection
        :param num_shards: number of shards for the collection
        :return: None
        """
        url = self.host + "admin/collections" + "?action=create&name={0}&numShards={1}"
        url = url.format(self.collection, num_shards)
        print url
        response = requests.get(url)
        print response

    def start_index(self, file_path, file_format='solrxml'):
        """
        Indexes data to its collection
        :param file_path: str
        :param file_format: str
        :return: None
        """
        url = self.host + self.collection + "/update/"
        #data1 = "<add><doc><field name='id'>{0}</field><field name='statement_s'>'How is it going?'</field><field name='response_s'>'It is good'</field></doc><doc><field name='id'>1234</field><field name='statement_s'>'How is it going-2?'</field><field name='response_s'>'It is good'</field></doc></add>".format(str(uuid.uuid4()))
        #print data1
        headers = {'Content-type': 'text/xml'}
        #requests.post(url, data=data1, headers=headers)
        data = self._xmltostr(file_path)
        print data
        requests.post(url, data=data, headers=headers)

    def _xmltostr(self, file_path):
        """

        :param file_path:
        :return: str
        """
        string = ""
        fh = open(file_path, 'r')
        for line in fh:
            string = string + line.strip(' ').rstrip('\n')

        fh.close()
        return string



















