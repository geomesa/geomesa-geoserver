/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.kafka.status

import org.geoserver.catalog.event.CatalogPostModifyEvent
import org.geoserver.catalog.{Catalog, DataStoreInfo}
import org.locationtech.geomesa.kafka.data.{KafkaCacheLoader, KafkaDataStoreParams}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.springframework.http.HttpStatus

import java.util.Collections

class KafkaLoadStatusControllerTest extends Specification with Mockito {

  sequential // to avoid conflicts with global KafkaCacheLoader.LoaderStatus

  "KafkaLoadStatusController" should {
    "load stores" in {
      val controller = new KafkaLoadStatusController()
      val catalog = mock[Catalog]
      catalog.getDataStores returns Collections.emptyList()
      controller.setCatalog(catalog)
      controller.afterPropertiesSet()
      there was one(catalog).addListener(controller)
      controller.status().getStatusCode mustEqual HttpStatus.OK
    }
    "return error if stores are not loaded" in {
      val kafkaInfo = mock[DataStoreInfo]
      kafkaInfo.getConnectionParameters returns Collections.singletonMap(KafkaDataStoreParams.Brokers.key, "localhost")
      kafkaInfo.getDataStore(null) answers (_ => { KafkaCacheLoader.LoaderStatus.startLoad(); null })
      val otherInfo = mock[DataStoreInfo]
      otherInfo.getConnectionParameters returns Collections.singletonMap("otherKey", "localhost")
      val controller = new KafkaLoadStatusController()
      val catalog = mock[Catalog]
      catalog.getDataStores returns java.util.List.of(kafkaInfo, otherInfo)
      controller.setCatalog(catalog)
      controller.afterPropertiesSet()
      there was one(catalog).addListener(controller)
      eventually(there was one(kafkaInfo).getConnectionParameters)
      eventually(there was one(kafkaInfo).getDataStore(null))
      eventually(there was one(otherInfo).getConnectionParameters)
      controller.status().getStatusCode mustEqual HttpStatus.SERVICE_UNAVAILABLE
      KafkaCacheLoader.LoaderStatus.completedLoad()
      eventually(controller.status().getStatusCode mustEqual HttpStatus.OK)
      there was no(otherInfo).getDataStore(null)
      val event = mock[CatalogPostModifyEvent]
      event.getSource returns kafkaInfo
      controller.handlePostModifyEvent(event)
      controller.status().getStatusCode mustEqual HttpStatus.SERVICE_UNAVAILABLE
      KafkaCacheLoader.LoaderStatus.completedLoad()
      eventually(controller.status().getStatusCode mustEqual HttpStatus.OK)
    }
  }
}
