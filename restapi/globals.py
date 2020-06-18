from requests import Session

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
