import requests
import csv
import uuid
import multiprocessing as mp
import time


class SolrClient(object):

    def __init__(self, host, version=4.7):
        """Constructor for SolrClient class

        Parameters
        ----------

        host : str
            Solr host
            Example:http://example.company.com:8983/solr/
        version : float
            Current version of Solr host, default=4.7
        """
        self.host = host
        self.version = version

    def get_collection(self, collection, max_rows=50000):
        """Factory method to return SolrCollection object

        Parameters
        ----------
        collection : str
            name of Solr collection
        max_rows : int
            maximum rows to fetch, default=50,000

        Returns
        -------
            SolrCollection
        """

        return SolrCollection(self.host, collection, max_rows)

    def get_control(self, collection):
        """Factory method to return SolrControl object

        Parameters
        ----------
        collection : str
            name of Solr collection

        Returns
        -------
            SolrControl
        """

        return SolrControl(self.host, collection)


class SolrCollection(SolrClient):
    """SolrCollection class

    Should not be instantiated directly. Use get_collection method of
    SolrClient object to get SolrCollection object
    """

    def __init__(self, host, collection, max_rows=50000):
        """Constructor for SolrCollection class

        Parameters
        ----------
        host : str
            Solr host
            Example:http://example.company.com:8983/solr/
        collection : str
            name of Solr collection
        max_rows : int
            maximum rows to fetch
        """
        SolrClient.__init__(self, host)
        self.collection = collection
        self.max_rows = max_rows
        self.last_call = None
        self.num_found = 0

    def pre_fetch(self, query, fields):
        """Fetches the first 10 rows from returned results from your Solr collection

        Parameters
        ----------
        query : str
            Query string
            Example: ``'field1':'val1' AND 'field2':'val2'``
        fields : list of str
            comma separated list of field names
            Example: ``['field1', 'field3']``

        Returns
        -------
            None
        """

        base_url = self.host + '{0}/select?'.format(self.collection)
        query_params = 'q=' + query + '&fl=' + fields + '&rows=10' + '&wt=json&indent=false'
        full_url = base_url + query_params
        self.last_call = full_url
        solr_response = requests.get(full_url).json()
        self.num_found = solr_response['response']['numFound']
    
    def fetch(self, query, fields=None, num_rows=None):
        """Fetches all rows from returned results from your Solr collection

        Parameters
        ----------
        query : str
            Query string
            Example: ``'field1':'val1' AND 'field2':'val2'``
        fields : list of str
            comma separated list of field names
            Example: ``['field1', 'field3']``
        num_rows : int
            number of rows to fetch

        Returns
        -------
            list
                a list of dicts
            None
                if self.num_found exceeds self.max_rows
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
        """Gets basic statistics from Solr

        Parameters
        ----------

        query : str
            Query string::
            Example: ``'field1':'val1' AND 'field2':'val2'``
        fields : list of str
            comma separated list of field names::
            Example: ``['field1', 'field3']``
        metrics : list of str
            list of available metrics are: 'min', 'max', 'sum', 'count', 'missing',
            'sumOfSquares', 'mean', 'stddev', 'percentiles', 'distinctValues',
            'countDistinct', 'cardinality'
        percentiles : str
            A string where different percentile values are separated by commas
            Example: ``"25,50,75"``
            Note: Uses t-digest approximation algorithm

        Returns
        -------
            dict
                A dictionary with metrics as keys
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

        Parameters
        ----------
        query : str
            Query string
            Example: ``'field1':'val1' AND 'field2':'val2'``
        field_params : dict
            Example: ``{field_1:[start, end, gap, include], field_2:[start, end, gap, include]}``
        bins : int

        Returns
        -------
            dict
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
        print(solr_response)
        documents = solr_response['facet_counts']['facet_ranges']
        return documents

    def __repr__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url

    def __str__(self):
        base_url = self.host + '{0}/select?'.format(self.collection)
        return base_url


class SolrControl(SolrClient):
    """SolrControl class can be used to make collections
    and perform indexing of your data.

    The data can be in a delimited file such as CSV or
    a Solr acceptable xml format such as::

        <add>
            <doc>
                <field name="id">001</field>
                <field name="food">milk</field>
                <field name="talk">meow</field>
            </doc>
            <doc>
                <field name="id">002</field>
                <field name="food">bone</field>
                <field name="talk">bark</field>
            </doc>
        </add>
    """

    def __init__(self, host, collection):
        """Constructor for SorControl class

        Parameters
        ----------
        host : str
            Solr host
            Example:http://example.company.com:8983/solr/
        collection : str
            name of Solr collection
        """
        SolrClient.__init__(self, host)
        self.collection = collection

    def make_collection(self, num_shards):
        """Makes a new collection
        This assumes that the user has already uploaded the
        collection's configuration to zookeeper

        Parameters
        ----------
        num_shards : int
            number of shards for the collection

        Returns
        -------
            None
        """
        url = self.host + "admin/collections" + "?action=create&name={0}&numShards={1}"
        url = url.format(self.collection, num_shards)
        response = requests.get(url)
        print(response)

    def start_index(self, file_path, file_format='solrxml',
                    delimiter=None, fields=None, unique_id=True, keep_row=False):
        """Indexes data to the collection

        Parameters
        ----------
        file_path : str
            Points to a file with data to be indexed
        file_format : str
            Available choices are 'solrxml' or 'csv'.
        delimiter : str
            Required when file_format='csv'. Example: ``","``
        fields : list of str.
            A list of field names to be used for indexing
            Example: ``['field1', 'field3']``
        unique_id : bool
            If True, autogenerates a field name id and a unique uuid value to the doc
            If False, modify the Solr config so that id is not a unique key

        Returns
        -------
            None
        """
        pool = mp.Pool()  # if processes argument is None, it will use cpu_count

        if file_format == 'solrxml':
            data = self._xmltostr(file_path)
            self._post_to_collection(data)

        if file_format == 'csv':
            if delimiter is not None and fields is not None:
                data_gen = self._data_iter(file_path, delimiter=delimiter,
                                           fields=fields,
                                           unique_id=unique_id,
                                           keep_row=keep_row)
                for data in data_gen:
                    pool.apply_async(self._post_to_collection, args=(data,))

                pool.close()
                pool.join()
            else:
                raise "csv file_format must have not None delimiter"

    def _post_to_collection(self, data):
        """Given the ``data`` in Solr acceptable xml format posts the data
        to the Solr Collection
        """
        url = self.host + self.collection + "/update/"
        headers = {'Content-type': 'text/xml'}
        requests.post(url, data=data, headers=headers)

    def _xmltostr(self, file_path):
        """Reads a solrxml file and converts it to a string

        Parameters
        ----------
        file_path : str
            An xml file

        Returns
        -------
            str
        """
        string = ""
        fh = open(file_path, 'r')
        for line in fh:
            string = string + line.strip(' ').rstrip('\n')

        fh.close()
        return string

    def _data_iter(self, file_path, delimiter=None, fields=None,
                   unique_id=True, keep_row=False):
        """Returns a generator of the read delimited file

        Parameters
        ----------
        file_path : str
            A delimited file
        delimiter : str
            Example: ``","``
        fields : list of str.
            A list of field names to be used for indexing
            Example: ``['field1', 'field3']``

        Yields
        -------
            str
                The next str is an xml formatted str with values
                read from a row in the ``file_path`` file.
                Example:
                if a delimited file contains a row as::

                    "cat", "milk", "meow"

                this method will yield::

                    <add>
                        <doc>
                            <field name="id">3d144141'</field>
                            <field name="food">Hi</field>
                            <field name="talk">Hello</field>
                        </doc>
                    </add>

                assuming the given fields are ``["food", "talk"]``
        """
        if keep_row:
            fields.append("row")

        csv_gen = self._csv_iter(file_path, delimiter=delimiter)
        for values in csv_gen:
            values = self._clean(values)

            if keep_row:
                values.append("|".join(values))

            data = self._get_data(values, fields, unique_id=unique_id)
            yield data

    def _csv_iter(self, filename, delimiter=','):
        """Returns a generator of the read delimited file

        Parameters
        ----------
        filename : str
            A delimited file
        delimiter : str
            Example: ``","``

        Yields
        -------
            list
                The next list of values read in a row in the given delimited file
        """
        with open(filename) as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for row in reader:
                yield row

    def _get_data(self, values, fields, unique_id=True):
        """Given the values and fields, returns an str in
        Solr acceptable xml format

        Parameters
        ----------
        values : list
            list of some data
        fields : list of str
            A list of field names to be used for indexing::
            Example: ``['field1', 'field3']``

        Returns
        -------
            str
        """
        d = {}
        for idx, value in enumerate(values):
            d[fields[idx]] = value
        return "<add>" + self._get_doc(d, unique_id=unique_id) + "</add>"

    def _get_doc(self, d, unique_id=True):
        """Given a dictionary of  fields and values, returns an str
        to be used by ``_get_data`` method
        """
        docs = ""
        if unique_id:
            docs = "<field name='id'>{0}</field>".format(uuid.uuid4())

        for k, v in d.items():
            docs = docs + "<field name='{0}'>{1}</field>".format(k, v)

        return "<doc>" + docs + "</doc>"

    def _clean(self, values):
        """Cleans the data in ``values``

        Parameters
        ----------
        values : list
            list of some data

        Returns
        -------
            list
                A list of values in ``values`` with leading and trailing
                whitespaces removed
        """
        return [value.strip() for value in values]
