# geomesa-gs-kafka-catalog-listener
This module provides two related capabilities.

First, there is a GeoServer catalog listener which starts reading Kafka topics 
for each registered layer on GeoServer start.  Without this plugin, requests will 
need to be made in order for Kafka DataStores and FeatureSources to be 
created (and hence for topics to be consumed).

Second, there is a readiness check available as a REST endpoint which
indicates when any initial Kafka loads have completed.  In order for
this to matter, Kafka DataStores would need to be configured
with a non-zero readback.  

To understand more about initial loading in a Kafka DataStore, setting
the log level for the `org.locationtech.geomesa.kafka.data` package
to `INFO` can help.