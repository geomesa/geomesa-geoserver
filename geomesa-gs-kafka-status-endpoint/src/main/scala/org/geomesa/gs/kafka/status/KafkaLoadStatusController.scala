/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.kafka.status

import com.typesafe.scalalogging.StrictLogging
import org.geoserver.catalog.event._
import org.geoserver.catalog.{Catalog, DataStoreInfo, FeatureTypeInfo}
import org.geoserver.rest.RestBaseController
import org.locationtech.geomesa.kafka.data.{KafkaCacheLoader, KafkaDataStoreParams}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RestController}

import java.util.Objects
import scala.util.Try

@RestController
@RequestMapping(path = Array("/rest/kafka"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
class KafkaLoadStatusController extends RestBaseController with CatalogListener with InitializingBean with StrictLogging {

  import scala.collection.JavaConverters._

  @Autowired
  private var catalog: Catalog = _

  @volatile
  private var loaded: Boolean = false

  @GetMapping
  // noinspection ScalaUnusedSymbol
  def status(): ResponseEntity[String] = {
    if (loaded && KafkaCacheLoader.LoaderStatus.allLoaded()) {
      new ResponseEntity("", HttpStatus.OK)
    } else {
      new ResponseEntity("Kafka layers are still loading", HttpStatus.SERVICE_UNAVAILABLE)
    }
  }

  override def afterPropertiesSet(): Unit = {
    catalog.addListener(this)
    reloaded()
  }

  override def handleAddEvent(event: CatalogAddEvent): Unit = loadStore(event)
  // note: called before the store is actually updated, wait for post-modify instead
  override def handleModifyEvent(event: CatalogModifyEvent): Unit = {}
  override def handlePostModifyEvent(event: CatalogPostModifyEvent): Unit = loadStore(event)
  override def handleRemoveEvent(event: CatalogRemoveEvent): Unit = {}

  override def reloaded(): Unit = {
    logger.info("Starting to load all datastores")
    val start = System.currentTimeMillis()
    CachedThreadPool.submit(() => {
      try {
        val futures = catalog.getDataStores.asScala.toList.map { dsi =>
          CachedThreadPool.submit(() => loadStore(dsi))
        }
        futures.foreach(_.get)
        logger.info(s"Finished loading datastores in ${System.currentTimeMillis() - start}ms")
      } finally {
        loaded = true
      }
    })
  }

  private def loadStore(event: CatalogEvent): Unit = {
    logger.debug(s"Received event: $event")
    event.getSource match {
      case dsi: DataStoreInfo   => loadStore(dsi)
      case fti: FeatureTypeInfo => loadStore(fti.getStore)
      case _ => // not a new layer - no action necessary
    }
  }

  private def loadStore(dsi: DataStoreInfo): Unit = {
    try {
      val params = dsi.getConnectionParameters
      if (params != null && KafkaDataStoreParams.Brokers.exists(params) && KafkaDataStoreParams.ConsumerReadBack.exists(params)) {
        val start = System.currentTimeMillis()
        val ds = dsi.getDataStore(null) // note: this gets a cached instance
        if (ds != null) {
          ds.getNames.asScala.foreach(ds.getFeatureSource)
        }
        logger.debug(s"Loaded store ${name(dsi)} in ${System.currentTimeMillis() - start}ms")
      }
    } catch {
      case e: Throwable => logger.error(s"Error loading store ${name(dsi)}:", e)
    }
  }

  private def name(dsi: DataStoreInfo): String =
    Try(s"${dsi.getWorkspace.getName}:${dsi.getName}").getOrElse(Objects.toString(dsi))

  def setCatalog(catalog: Catalog): Unit = this.catalog = catalog
  def getCatalog: Catalog = this.catalog
}
