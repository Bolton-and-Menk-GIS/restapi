import json
import sys
import inspect

"""Custom error classes to transform the
200  status code returned by REST endpoints
based on the actual content.
This may not be complete, but it is based on 
this published listing of error codes:
https://developers.arcgis.com/net/reference/platform-error-codes/#http-network-and-rest-errors

"""

class RestAPIException(Exception):
    restapiexception = True
    code = -1
    def __init__(self, err):
        self.json = err['error']
        self.json_string = json.dumps(self.json, indent=2, ensure_ascii=False)
        self.code = self.json.get('code')
        self.message = self.json.get('message')
        self.details = self.json.get('details')

        
class RestAPIUnableToCompleteOperationException(RestAPIException):
    """REST Exception for Code 400:
    Unable to complete operation.
    """
    code = 400
    def __init__(self, err):
        raise RestAPIException(err)

    
class RestAPIAuthorizationRequiredException(RestAPIException):
    """REST Exception for Code 401:
    Authorization to the requested resource is required.
    """
    code = 401
    def __init__(self, err):
        raise RestAPIException(err)

    
class RestAPITokenValidAccessDeniedException(RestAPIException):
    """REST Exception for Code 403:
    Token is valid but access is denied.
    """
    code = 403
    def __init__(self, err):
        raise RestAPIException(err)
    
    
class RestAPINotFoundException(RestAPIException):
    """REST Exception for Code 404:
    The requested resource was not found.
    """
    code = 404
    def __init__(self, err):
        raise RestAPIException(err)

    
class RestAPITooLargeException(RestAPIException):
    """REST Exception for Code 413:
    The request is larger than limits defined by the server.
    If you're trying to upload an attachment, 
    this error might indicate that the attachment's size exceeds the 
    maximum size allowed.
    """
    code = 413
    def __init__(self, err):
        raise RestAPIException(err)

    
class RestAPIInvalidTokenException(RestAPIException):
    """REST Exception for Code 498:
    The access token provided is invalid or expired.
    """
    code = 498
    def __init__(self, err):
        raise RestAPIException(err)
    

class RestAPITokenRequiredException(RestAPIException):
    """REST Exception for Code 499:
    Token required but not passed in the request.
    """
    code = 499
    def __init__(self, err):
        raise RestAPIException(err)
        

class RestAPIErrorPerforningOperationException(RestAPIException):
    """REST Exception for Code 500:
    Error performing <operation name> operation.
    """
    code = 500
    def __init__(self, err):
        raise RestAPIException(err)


class RestAPINotImplementedException(RestAPIException):
    """REST Exception for Code 501:
    The requested service is not implemented.
    """
    code = 501
    def __init__(self, err):
        raise RestAPIException(err)
    

class RestAPIGatewayTimeoutException(RestAPIException):
    """REST Exception for Code 504:
    Gateway Timeout.
    """
    code = 504
    def __init__(self, err):
        raise RestAPIException(err)

    
# make a dictionary of our custom error classes
exception_lookup = {e.code: e for _, e in inspect.getmembers(
    sys.modules[__name__], 
    lambda member: inspect.isclass(member) and 
        member.__module__ == __name__ and 
        getattr(member, 'restapiexception', False))}


class RequestError(object):
    """Class to handle restapi request errors."""
    def __init__(self, err):
        if 'error' in err:
            code = err['error'].get('code')
            if code in exception_lookup:
                raise exception_lookup[code](err)
            else:
                raise RestAPIException(err)

AuthExceptionCodes = [401, 403, 498, 499]