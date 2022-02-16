# geomesa-gs-monitor-elasticsearch

A GeoServer plugin to write request data produced by the
[GeoServer Monitor Extension](https://docs.geoserver.org/latest/en/user/extensions/monitoring/index.html)
to Elasticsearch.

All fields from the
[RequestData](https://github.com/geoserver/geoserver/blob/main/src/extension/monitor/core/src/main/java/org/geoserver/monitor/RequestData.java)
object are serialized to JSON and written to Elasticsearch. The fields are keyed by their Java name, but lowercase and
separated by underscores.

Additionally, some extra fields are computed from the request:
 * `failed` – whether the request's `error` field is populated
 * `bboxCentroid` – the centroid of the request's `bbox` 
 * `queryCentroids` – a list of centroids for each geometry in the request's `queryString`

### Deployment

Add the `jar` produced to your GeoServer `/WEB-INF/lib` directory or include this plugin as a dependency in your
`pom.xml`

You must enable the `elasticsearch` profile in your `spring.profiles.active` configuration.

### Configuration

> The Elasticsearch index and mapping should be created before starting GeoServer with this plugin.

The plugin reads from a Typesafe Config file, `application.conf`, under the key
`geomesa.geoserver.monitor.elasticsearch`:
  * `host` – The host of the Elasticsearch instance
  * `port` – The port of the Elasticsearch instance
  * `user` – Your Elastic username
  * `password` – Your Elastic password
  * `index` – The index to write requests to
  * `excludedFields` – A list of fields names that should not be written to Elasticsearch, default is none
                       (these field names should match those of the Java object)

##### Sample Config

```
geomesa.geoserver.monitor.elasticsearch = {
  host = "localhost"
  port = 9200
  user = "elastic"
  password = "password"
  index = "requests"
  excludedFields = [ "internalid", "id", "status", "path", "remoteCountry", "remoteCity", "remoteLat", "remoteLon", "owsVersion", "cacheResult", "missReason", "resourcesProcessingTime", "labellingProcessingTime" ]
}
```
