import json
import unittest

import requests
from requests.adapters import BaseAdapter

import restapi as r
from restapi import rest_utils


SERVICE_URL = 'https://example.com/arcgis/rest/services/Demo'


class ServiceAdapter(BaseAdapter):
    """Returns service metadata and redirects without making network requests."""
    def __init__(self):
        self.requests = []

    def send(self, request, **kwargs):
        self.requests.append(request)
        response = requests.Response()
        response.request = request
        response.url = request.url
        response.status_code = 200
        if '/redirect' in request.url:
            response.status_code = 302
            response.headers['Location'] = SERVICE_URL + '/MapServer'
            response._content = b''
        elif '/query' in request.url:
            response._content = b'{"count": 0}'
        else:
            response._content = json.dumps({
                'layers': [{'id': 0, 'name': 'Roads'}],
                'tables': [{'id': 1, 'name': 'Owners'}],
                'type': 'Table',
                'fields': []
            }).encode('utf-8')
        return response

    def close(self):
        pass


class TestRequestContext(unittest.TestCase):
    def setUp(self):
        self.original_client = rest_utils.requestClient
        self.original_proxies = rest_utils.ID_MANAGER.proxies.copy()
        self.session = requests.Session()
        self.adapter = ServiceAdapter()
        self.session.mount('https://', self.adapter)
        self.client = r.RequestClient(self.session)
        self.default_session = requests.Session()
        self.default_adapter = ServiceAdapter()
        self.default_session.mount('https://', self.default_adapter)
        r.set_request_client(r.RequestClient(self.default_session))

    def tearDown(self):
        rest_utils.requestClient = self.original_client
        rest_utils.ID_MANAGER.proxies.clear()
        rest_utils.ID_MANAGER.proxies.update(self.original_proxies)
        self.session.close()
        self.default_session.close()

    def check_factory(self, service_class, service_type, factory, selectors, layer_class):
        for proxy in (None, 'https://example.com/proxy.ashx'):
            service = service_class(SERVICE_URL + '/' + service_type,
                token='test-token', proxy=proxy, referer='https://example.com/app',
                client=self.client)
            for selector in selectors:
                layer = getattr(service, factory)(selector)
                self.assertIsInstance(layer, layer_class)
                self.assertIs(layer.client, service.client)
                self.assertEqual(layer.token, service.token)
                self.assertEqual(layer._proxy, service._proxy)
                self.assertEqual(layer._referer, service._referer)
                layer_request = self.adapter.requests[-1]
                self.assertIn('/' + str(selectors[0]), layer_request.url)
                self.assertEqual(layer.query(f='json', returnCountOnly=True).count, 0)
                if proxy:
                    self.assertTrue(layer_request.url.startswith(proxy + '?'))
                    self.assertEqual(layer_request.headers['Referer'], service._referer)
                    self.assertEqual(self.adapter.requests[-1].headers['Referer'], service._referer)
            self.assertEqual(self.default_adapter.requests, [])

    def test_map_service_layer(self):
        self.check_factory(r.MapService, 'MapServer', 'layer', (0, 'Road*'), r.MapServiceLayer)

    def test_feature_service_layer(self):
        self.check_factory(r.FeatureService, 'FeatureServer', 'layer', (0, 'Road*'), r.FeatureLayer)

    def test_map_service_table(self):
        self.check_factory(r.MapService, 'MapServer', 'table', (1, 'Owner*'), r.MapServiceTable)

    def test_feature_service_table(self):
        self.check_factory(r.FeatureService, 'FeatureServer', 'table', (1, 'Owner*'), r.MapServiceTable)

    def test_redirects_are_followed_by_default(self):
        response = r.do_request(SERVICE_URL + '/redirect', client=self.client, ret_json=False)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(self.adapter.requests), 2)

    def test_redirects_can_be_disabled(self):
        for method in ('get', 'post'):
            self.adapter.requests[:] = []
            response = r.do_request(SERVICE_URL + '/redirect', params={'f': 'json'},
                client=self.client, method=method, allow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(len(self.adapter.requests), 1)
            self.assertNotIn('allow_redirects', self.adapter.requests[0].url)
            self.assertNotIn('allow_redirects', self.adapter.requests[0].body or '')

    def test_proxy_redirects_can_be_disabled(self):
        response = r.do_request(SERVICE_URL + '/MapServer', params={'f': 'json'},
            proxy=SERVICE_URL + '/redirect', client=self.client, allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.adapter.requests), 1)
        self.assertNotIn('allow_redirects', self.adapter.requests[0].body or '')

    def test_proxy_get_redirects_can_be_disabled(self):
        response = rest_utils.do_proxy_request(SERVICE_URL + '/redirect',
            SERVICE_URL + '/MapServer', params={'f': 'json'}, method='get',
            client=self.client, allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.adapter.requests), 1)
        self.assertNotIn('allow_redirects', self.adapter.requests[0].url)

    def test_endpoint_request_can_disable_redirects(self):
        service = r.MapService(SERVICE_URL + '/MapServer', client=self.client)
        response = service.request(SERVICE_URL + '/redirect', allow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.adapter.requests), 2)

    def test_redirect_option_is_not_a_service_parameter(self):
        for method in ('get', 'post'):
            for allow_redirects in (True, False):
                params = {'f': 'json'}
                r.do_request(SERVICE_URL + '/MapServer', params=params,
                    client=self.client, method=method, allow_redirects=allow_redirects)
                self.assertNotIn('allow_redirects', params)
                self.assertNotIn('allow_redirects', self.adapter.requests[-1].url)
                self.assertNotIn('allow_redirects', self.adapter.requests[-1].body or '')


if __name__ == '__main__':
    unittest.main()
