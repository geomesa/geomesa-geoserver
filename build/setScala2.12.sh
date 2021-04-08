sed -i -e "s#geomesa-geoserver</art#geomesa-geoserver_2.12</art#g" \
-e "s#scala.version>2.11.7#scala.version>2.12.13#g" \
-e "s#scala.binary.version>2.11#scala.binary.version>2.12#g" \
-e "s#geomesa-gs-catalog-listener</art#geomesa-gs-catalog-listener_2.12</art#g" \
-e "s#geomesa-gs-kafka-catalog-listener</art#geomesa-gs-kafka-catalog-listener_2.12</art#g" \
-e "s#geomesa-gs-monitor-elasticsearch</art#geomesa-gs-monitor-elasticsearch_2.12</art#g" \
-e "s#geomesa-gs-styling</art#geomesa-gs-styling_2.12</art#g" \
-e "s#geomesa-gs-wfs</art#geomesa-gs-wfs_2.12</art#g" \
pom.xml geomesa-gs-catalog-listener/pom.xml geomesa-gs-kafka-catalog-listener/pom.xml geomesa-gs-monitor-elasticsearch/pom.xml geomesa-gs-styling/pom.xml geomesa-gs-wfs/pom.xml
