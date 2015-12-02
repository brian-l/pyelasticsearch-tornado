import json

from pyelasticsearch import ElasticSearch
from pyelasticsearch.client import es_kwargs
from pyelasticsearch.exceptions import InvalidJsonResponseError
from six import iteritems
from tornado import gen
from tornado import ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from urllib import urlencode

class AsyncElasticSearch(ElasticSearch):
    client = AsyncHTTPClient()

    @gen.coroutine
    def send_request(self,
                     method,
                     path_components,
                     body='',
                     query_params=None,
                     encode_body=True):
        """
        Send an asynchronous HTTP request to ES via tornado,
        and return the JSON-decoded response.

        :arg method: An HTTP method, like "GET"
        :arg path_components: An iterable of path components, to be joined by
            "/"
        :arg body: The request body
        :arg query_params: A map of querystring param names to values or
            ``None``
        :arg encode_body: Whether to encode the body of the request as JSON
        """
        path = self._join_path(path_components)
        if query_params:
            path = '?'.join(
                [path,
                 urlencode(dict((k, self._utf8(self._to_query(v))) for k, v in
                                iteritems(query_params)))])

        request_body = self._encode_json(body) if encode_body else body
        server_url, was_dead = self.servers.get()
        url = "%s%s" % (server_url, path)

        request = HTTPRequest(
            url,
            method = method.upper(),
            headers = {
                'Accept': 'application/json',
                'Content-type': 'application/json',
            },
            connect_timeout = self.timeout or 5,
            request_timeout = self.timeout or 30,
            allow_nonstandard_methods = True, # this is required to have request body to GET
        )

        if body:
            request.body = request_body

        self.logger.debug(
            "Making a request equivalent to this: curl -X%s '%s' -d '%s'",
            method, url, request_body,
        )

        for attempt in xrange(self.max_retries + 1):
            try:
                response = yield self.client.fetch(request)
            except HTTPError, he:
                if attempt >= self.max_retries:
                    raise

                self.logger.error(
                    "HTTP %d (%s) from %s. %d more attempts." % (
                        he.code, he.message, server_url, (self.max_retries - attempt)
                    )
                )


        self.logger.debug('response status: %s', response.code)
        prepped_response = self._decode_response(response.body)
        if response.code >= 400:
            self._raise_exception(response, prepped_response)
        self.logger.debug('got response %s', prepped_response)

        raise gen.Return(prepped_response)

    def _decode_response(self, response):
        """Return a native-Python representation of a response's JSON blob."""
        try:
            json_response = json.loads(response)
        except ValueError:
            raise InvalidJsonResponseError(response)
        return json_response

    def _search_or_count(self, kind, query, index=None, doc_type=None, query_params=None):
        if isinstance(query, basestring):
            query_params['q'] = query
            body = ''
        else:
            body = query

        if body:
            method = 'POST'
        else:
            method = 'GET'

        return self.send_request(
            method,
            [self._concat(index), self._concat(doc_type), kind],
            body,
            query_params = query_params
        )

    @es_kwargs()
    def percolate(self, index, doc_type, doc, query_params=None):
        # Replace GET with POST for percolation
        return self.send_request(
            'POST',
            [index, doc_type, '_percolate'],
            doc, query_params = query_params
        )

    @es_kwargs()
    def multi_get(self, docs, query_params=None):
        return self.send_request('POST', ['_mget'], {'docs': docs}, query_params=query_params)

    """
    Bulk update monkeypatch (requires at least elasticsearch 0.90.6 - tested up to 1.4)

    Changed the hardcoded 'index' action to 'update', and dropped the es_kwargs decorator.

    This is a hack.
    """
    def bulk_update(inst, index, doc_type, docs, id_field='id',
                   parent_field='_parent', routing_field='_routing', query_params=None):
        body_bits = []
        if query_params is None:
            query_params = {}

        if not docs:
            raise ValueError('No documents provided for bulk indexing!')

        for doc in docs:
            action = {'update': {'_index': index, '_type': doc_type}}

            if doc.get(routing_field):
                action['update']['_routing'] = doc.pop(routing_field)

            if doc.get(id_field) is not None:
                action['update']['_id'] = doc[id_field]

            if doc.get(parent_field) is not None:
                action['update']['_parent'] = doc.pop(parent_field)

            body_bits.append(inst._encode_json(action))
            body_bits.append(inst._encode_json({
                'doc': doc,
                'doc_as_upsert': True, # this is the magic. update-insert the whole document
            }))

        # Need the trailing newline.
        body = '\n'.join(body_bits) + '\n'
        return inst.send_request('POST',
             ['_bulk'],
             body,
             encode_body=False,
             query_params=query_params
         )

