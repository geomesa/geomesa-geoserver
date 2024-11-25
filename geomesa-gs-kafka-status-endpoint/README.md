# geomesa-gs-kafka-status-endpoint

This module provides a readiness check available as a REST endpoint which indicates when any initial Kafka loads have
completed. In order for this to matter, Kafka DataStores need to have `kafka.consumer.start-on-demand` set to false and
`kafka.consumer.read-back` set to a non-zero value.

The endpoint is available at `/geoserver/rest/kafka` (assuming a default context path).
