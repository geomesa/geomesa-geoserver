/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.catalog.kafka

import com.typesafe.scalalogging.LazyLogging
import org.geoserver.catalog.event._
import org.geoserver.catalog.{Catalog, FeatureTypeInfo}
import org.geotools.data.DataStore
import org.geotools.data.store.DecoratingDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.opengis.feature.`type`.FeatureType
import org.springframework.beans.factory.InitializingBean

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * This CatalogListener starts consumers on GeoServer start up for
 * the KDS layers which have been registered as layers.
 */
class GeoMesaKafkaCatalogListener extends CatalogListener with InitializingBean with LazyLogging {

  private var catalog: Catalog = _

  def setCatalog(catalog: Catalog): Unit = this.catalog = catalog
  def getCatalog: Catalog = this.catalog

  override def afterPropertiesSet(): Unit = {
    logger.debug("Starting Catalog crawl for existing GeoMesa layers.")
    catalog.addListener(this)
    crawlCatalog()
    logger.debug(s"Finished Catalog crawl for existing GeoMesa layers, and added listener to catalog: $catalog.")
  }

  def crawlCatalog(): Unit = {
    CachedThreadPool.execute(new Runnable {
      override def run(): Unit = {
        catalog.getFeatureTypes.foreach(addFeatureTypeInfo)
        logger.debug("Finished Catalog crawl for existing GeoMesa layers")
      }
    })
  }

  def addFeatureTypeInfo(featureTypeInfo: FeatureTypeInfo): Unit = {
    logger.debug(s"Got a datastore for adding a feature type info $featureTypeInfo")
    getStore(featureTypeInfo) match {
      case kds: KafkaDataStore =>
        //        val nativeName = featureTypeInfo.getNativeName
        //        val workspace = featureTypeInfo.getStore.getWorkspace.getName
        val layerName = featureTypeInfo.getName
        logger.debug(s"Starting Kafka consumer for layer: $layerName.")
        val fs = featureTypeInfo.getFeatureSource(null, null)
        val sft: FeatureType = fs.getSchema
        logger.debug(s"Finished starting Kafka consumer for layer: $layerName with SFT ${sft.getName}.")
      case s => logger.debug(s"Encountered non-GeoMesa store: ${Option(s).map(_.getClass.getName).orNull}")
    }
  }

  private def getStore(info: FeatureTypeInfo): DataStore =
    Try(unwrap(info.getStore.getDataStore(null).asInstanceOf[DataStore])).getOrElse(null)

  private def unwrap(store: DataStore): DataStore = store match {
    case ds: DecoratingDataStore => ds.unwrap(classOf[DataStore])
    case ds => ds
  }

  override def handleAddEvent(event: CatalogAddEvent): Unit = {
    logger.trace(s"GeoMesa Kafka Catalog Listener received add event: $event")
    event.getSource match {
      case fti: FeatureTypeInfo => addFeatureTypeInfo(fti)
      case _ => // not a new layer; no action necessary.
    }
  }

  override def handleRemoveEvent(event: CatalogRemoveEvent): Unit = {
    logger.trace(s"GeoMesa Kafka Catalog Listener received remove event: $event")
  }

  override def handlePostModifyEvent(event: CatalogPostModifyEvent): Unit = {
    logger.trace(s"GeoMesa Kafka Catalog Listener received post modify event: $event")
  }

  override def handleModifyEvent(event: CatalogModifyEvent): Unit = {
    logger.trace(s"GeoMesa Kafka Catalog Listener received modify event: $event")
  }

  override def reloaded(): Unit = {
    logger.trace(s"GeoMesa Kafka Catalog Listener received reloaded message.")
  }
}
