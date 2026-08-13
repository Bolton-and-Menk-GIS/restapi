import ssl
from requests import Session
from requests.adapters import HTTPAdapter
from six.moves.urllib_parse import urlparse

try:
    from urllib3.util.ssl_ import create_urllib3_context
except ImportError:
    from requests.packages.urllib3.util.ssl_ import create_urllib3_context

OP_LEGACY_SERVER_CONNECT = getattr(ssl, 'OP_LEGACY_SERVER_CONNECT', 0x4)


class LegacySSLAdapter(HTTPAdapter):
    """Transport adapter that allows "unsafe" legacy TLS renegotiation.

    OpenSSL 3+ refuses to connect to servers that do not support secure
    renegotiation (RFC 5746), raising
    'SSL: UNSAFE_LEGACY_RENEGOTIATION_DISABLED'. Many older ArcGIS Server
    deployments still run behind such SSL configurations, while browsers
    continue to allow them. Mounting this adapter restores the pre-OpenSSL 3
    behavior for the mounted prefix. Certificate verification is unaffected.
    """
    def _create_context(self):
        ctx = create_urllib3_context()
        ctx.options |= OP_LEGACY_SERVER_CONNECT
        return ctx

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self._create_context()
        return super(LegacySSLAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs['ssl_context'] = self._create_context()
        return super(LegacySSLAdapter, self).proxy_manager_for(*args, **kwargs)


def mount_legacy_ssl_adapter(session, url=None):
    """mounts a LegacySSLAdapter onto a session, scoped to the server of the
    given url (or all https traffic if no url is provided)

    Args:
        session: a requests.Session() instance
        url: Optional url, the adapter is only mounted for this url's server.
    """
    prefix = 'https://'
    if url:
        netloc = urlparse(url).netloc
        if netloc:
            prefix = 'https://{}/'.format(netloc)
    session.mount(prefix, LegacySSLAdapter())
    return session


class RequestClient(object):
    """Represents a RequestClient"""
    def __init__(self, session=None):
        if not session:
            session = Session()
        self.session = session

class DefaultRequestClient(RequestClient):
    """singleton for a DefaultRequestClient, should only be initialized once"""
    _instance = None
    session = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DefaultRequestClient, cls).__new__(cls, *args, **kwargs)
        return cls._instance
