import requests
import csv
import uuid
import multiprocessing as mp
import time


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

        :returns: SolrCollection object
        """

        return SolrCollection(self.host, collection, max_rows)

    def get_control(self, collection):
        """
        Generator method to return SolrCollection

        :param collection: string
                    name of Solr collection

        :returns: SolrIndexer object
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

        :returns: None
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

        :returns: a list of dicts or None if self.num_found exceeds self.max_rows
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
        """Gets basic statistics using Solr stats

        :param query: str
                    Example: 'field1:val1 AND field2:val2'
        :param fields: str. comma separated list of fields to compute stats on.
                    Example: [field1, field3]
        :param metrics: list of str list of metrics to be used
                    Must be in ['min', 'max', 'sum', 'count', 'missing', 'sumOfSquares'
                    'mean', 'stddev', 'percentiles', 'distinctValues', 'countDistinct',
                    'cardinality']
        :param percentiles: string of numbers separated by commas to calculate percentiles at
                    Uses t-digest approximation algorithm

        :returns: dict with metrics as keys
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
        """Get facet results using Solr Facets
        :param query: str

        :param field_params: dict
                        Example: {field_1:[start, end, gap, include], field_2:[start, end, gap, include]}

        :param bins: int

        :returns: dict
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
        This assumes that the user has already uploaded the collection's configuration to zookeeper

        :param name: name of the collection
        :param num_shards: number of shards for the collection

        :returns: None
        """
        url = self.host + "admin/collections" + "?action=create&name={0}&numShards={1}"
        url = url.format(self.collection, num_shards)
        print url
        response = requests.get(url)
        print response

    def start_index(self, file_path, file_format='solrxml', delimiter=None, fields=None):
        """
        Indexes data to its collection

        :param file_path: str
        :param file_format: str
        :param delimiter: None or str. Required when file_format='csv'
        :param fields: list of str. A list of field names

        :returns: None
        """
        pool = mp.Pool()  # if processes argument is None, it will use cpu_count

        if file_format == 'solrxml':
            data = self._xmltostr(file_path)
            self._post_to_collection(data)

        if file_format == 'csv':
            if delimiter is not None and fields is not None:
                data_gen = self._data_iter(file_path, delimiter=delimiter, fields=fields)
                for data in data_gen:
                    pool.apply_async(self._post_to_collection, args=(data,))
                    # self._post_to_collection(data)
            else:
                raise "csv file_format must have not None delimiter"

    def _post_to_collection(self, data):
        url = self.host + self.collection + "/update/"
        headers = {'Content-type': 'text/xml'}
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

    def _data_iter(self, file_path, delimiter=None, fields=None):
        csv_gen = self._csv_iter(file_path, delimiter=delimiter)
        for values in csv_gen:
            values = self._clean(values)
            data = self._get_data(values, fields)
            yield data

    def _csv_iter(self, filename, delimiter=','):
        """

        :param file_path: str
        :param delimiter: str
        :return: generator
        """
        with open(filename) as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for row in reader:
                yield row

    def _get_data(self, values, fields):
        d = {}
        for idx, value in enumerate(values):
            d[fields[idx]] = value
        return "<add>" + self._get_doc(d) + "</add>"

    def _get_doc(self, d):
        """

        :param d: dict of field names and value pairs
        :return:
        """
        docs = "<field name='id'>{0}</field>".format(uuid.uuid4())
        for k, v in d.items():
            docs = docs + "<field name='{0}'>{1}</field>".format(k, v)

        return "<doc>" + docs + "</doc>"

    def _clean(self, values):
        return [value.strip() for value in values]




















