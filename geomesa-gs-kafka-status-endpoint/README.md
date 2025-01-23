# geomesa-gs-kafka-status-endpoint

This module provides a readiness check available as a REST endpoint which indicates when any initial Kafka loads have
completed. In order for this to matter, Kafka DataStores need to have `kafka.consumer.read-back` set to a non-zero value.
Note that any layers with read-back will be eagerly loaded, ignoring the value of `kafka.consumer.start-on-demand`.

The endpoint is available at `/geoserver/rest/kafka` (assuming a default context path).
