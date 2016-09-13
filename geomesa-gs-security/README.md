## GeoMesa Web Security

GeoMesa Web Security provides Accumulo security to data stores within GeoServer, particularly the
KafkaDataStore but it works for any data store using the
``org.locationtech.geomesa.utils.security.DataStoreSecurityService``.  The easiest way for a data store to use
this service is to modify the ``FeatureSource`` to extend
``org.locationtech.geomesa.utils.geotools.ContentFeatureSourceSecuritySupport``.  Alternatively, the data
store may call ``org.locationtech.geomesa.utils.security.DataStoreSecurityService.provider.secure(obj)`` which
will provide a security wrapper around ``obj`` where ``obj`` may be a ``FeatureSource``, ``FeatureReader``,
or a ``FeatureCollection``.


To deploy the module, copy the ```geomesa-web-security-$VERSION.jar``` and dependencies to 
```$GEOSERVER_WEB_APP_HOME/WEB-INF/lib``` and restart GeoServer.  Any data store using the
``DataStoreSecurityService`` will then have an Accumulo visibility filter applied to SimpleFeatures.  The
visibility property in the simple feature ```user data``` will be compared to the current user's
authorizations to determine if the user has access to the simple feature or not.  Any simple feature with no
visibility will be inaccessable to all users.  This behavior differs from Accumulo where entries without a
visibility marking are accessible to all users.


The ``DataStoreSecurityService`` provides read security.  The data store should be additionally configured in
GeoServer to be read-only.  To configure read only access, log into GeoServer using administrator credentials.
Then select "Data" under "Security" on the left side menu.  Set the "Catalog Mode" to "Hide".  Next, click
the default "*.*.w" rule and add the roles "ADMIN" and optionally "GROUP_ADMIN".  Without this all users will
have write access by default.  Finally, create a new rule for the workspace and layer that you want to control
access to.  Set the access mode to "Read" and add all roles (i.e. visibility lables) that exist in the datset.
Don't forget to click "Save" on the main "Data Security" page when you're done.


To add additional users and roles select "User, Groups, Roles" under "Security" on the left side menu.


See ```org.locationtech.geomesa.security.SecurityUtils``` for more details on how to get and set
visibilities on a SimpleFeature

### Auditing

GeoMesa Security provides hooks into the GeoServer auditing framework. The auditor can write to any
MetaModel data context.

Auditing is disabled by default, as it requires the GeoServer monitoring extension.

#### Installation

To use, you must first install the GeoServer monitoring extension as detailed
[here](http://docs.geoserver.org/latest/en/user/extensions/monitoring/installation.html).

Edit the `geomesa-ext-gs-security-<version>.jar` jar and modify the embedded file `applicationContext.xml`.
Uncomment the existing spring beans. The default configuration will write to a CSV file under `/tmp`.
To modify this behavior, update the `auditDataContext` bean in `applicationContext.xml` to a different metamodel
data context.

Copy the modified `geomesa-ext-gs-security-<version>.jar` into `geoserver/WEB-INF/lib`. For the default CSV auditing,
also copy `MetaModel-core-4.5.3.jar` and `MetaModel-csv-4.5.3.jar`. For other MetaModel bindings, copy
`MetaModel-core-4.5.3.jar` and the appropriate implementation jar.
