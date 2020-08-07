# Major Versions

 1. [Version 1.0](#Version-1.0)
 2. [Version 2.0](#Version-2.0)

## Version 2.0
The major change in version 2.0 was the addition of a `Session` object. it is possible to use a custom `requests.Session()` instance. This instance can be defined globally for all requests made by `restapi`, or it can be passed on each function call as a `restapi.RequestClient()` object. This can be useful if different parameters are needed to access different servers.

Use this functionality to access servers behind HTTP or SOCKS proxies, to disable certificate validation or use custom CA certificates, or if additional authentication is needed. 

Due to the nature of ArcGIS Server deployments, `bmi-arcgis-restapi` was defaulting to use the arg `verify=False` in any web requests it made. While this is a security risk, it was necessary because many ArcGIS Server connections, particularly when connecting to an internal deployment, use self-signed SSL certificates. These connections would have otherwise been impossible. With the addition of the `restapi.RequestClient()`, it is now trivial for the user to set this manually if needed. Therefore, any instances of `verify=False` have been removed. **This will break existing code that relies on this functionality for connecting to servers with self-signed certificates!** For all other use cases, this presents a significant security enhancement. To maintain previous behavior, set `verify=False` on the `restapi.requestClient.session` object, or pass a specific certificate file (see [https://requests.readthedocs.io/en/master/user/advanced/#ssl-cert-verification](https://requests.readthedocs.io/en/master/user/advanced/#ssl-cert-verification))
The `restapi` readme contains example code for using the `restapi.RequestClient()` functionality.

## Version 1.0
Version 1.0 which was a major overhaul on this package to include many performance and convenience improvements.

#### New Classes:
	
	GeometryService
	GeometryCollection
	FeatureSet
	RelatedRecords
	Feature
	JsonReplica
	SQLiteReplica

#### New Base Classes/mixins:

	JsonGetter
	RestapiEncoder
	NameEncoder
	SpatialReferenceMixin
	FieldsMixin
	BaseGeometryCollection


#### Deprecated Functions (many of these have been converted to class methods):
	
	query
	query_all
	list_layers -> class method
	list_tables 
	list_fields
	walk
	objectize

#### Major changes:

 - Cursor -- this has been completely rewritten as an extension of the
   new FeatureSet class. The Row class is also no longer globally
   exposed and is now an attribute of the Cursor.
 - ArcServer -- in versions < 1.0, this would build the service list for
   the entire REST services directory at initialization, this no longer
   happens to improve performance.  The "services" property has also
   changed to represent just the services at the root level.  A new
   property has been implemented called "service_cache", which is
   initially empty upon initialization and gets populated whenever a
   call to iter_services, list_services, or walk is called.

#### Misc changes:

 - Package has been completely restructured, with all important classes
   being stored in "common_types.py".

  

 - FeatureLayer is now a subclass of MapServiceLayer, so now you get all
   the methods available in the MapServiceLayer that weren't previously
   available.
 - The query() method of MapServiceLayer/FeatureLayer "get_all" argument
   has been changed to "exceed_limit".  This parameter allows you to
   make repetitive calls to gather features by exceeding the
   maxRecordCount of the service.
 - The "layer_to_fc" method for MapServiceLayer and FeatureLayer has
   been renamed to "export_layer" with legacy alias support for
   "layer_to_fc".
 - There has also been a new parameter added to this function to include
   attachments (only supported if output is GDB Feature Class and user
   has access to arcpy).
