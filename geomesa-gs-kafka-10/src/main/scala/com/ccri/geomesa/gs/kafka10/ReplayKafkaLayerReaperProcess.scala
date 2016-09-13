/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package com.ccri.geomesa.gs.kafka10

import com.typesafe.scalalogging.LazyLogging
import org.geoserver.catalog.event._
import org.geoserver.catalog.{Catalog, LayerInfo}
import org.geotools.data.DataStore
import org.geotools.process.factory.{DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.control.NonFatal

@DescribeProcess(
  title = "GeoMesa Replay Kafka Layer Reaper",
  description = "Removes Kafka Replay Layers from GeoServer",
  version = "1.0.0"
)
class ReplayKafkaLayerReaperProcess(val catalog: Catalog, val hours: Int)
  extends GeomesaKafkaProcess with LazyLogging with Runnable {

  // register a listener to remove the schema when a replay layer is deleted
  // this will be triggered by both a user deleting a layer and the reaper process
  catalog.addListener(new ReplayKafkaLayerCatalogListener())

  @DescribeResult(name = "result",
                  description = "If all eligible layers were removed successfully, true, otherwise false.")
  def execute(): Boolean = {
    Try {
      val currentTime: Instant = new Instant(System.currentTimeMillis())
      val ageLimit: Instant = currentTime.minus(Duration.standardHours(hours))

      var error = false

      for {
        layer <- catalog.getLayers
        age <- VolatileLayer.getVolatileAge(layer)
        if age.isBefore(ageLimit)
      } {
        try {
          logger.debug(s"Deleting old replay layer $layer.")
          catalog.remove(layer)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error deleting old replay layer $layer.", e)
            error = true
        }
      }

      error
    }.getOrElse(false)
  }

  private val message = "Running Replay Kafka Layer Cleaner"

  override def run(): Unit = {
    logger.info(message)
    execute()
  }
}

class ReplayKafkaLayerCatalogListener extends CatalogListener with LazyLogging {

  override def handleRemoveEvent(event: CatalogRemoveEvent): Unit = event.getSource match {
    case layer: LayerInfo =>
      Try {
        for {
          sftName <- VolatileLayer.getSftName(layer)
          ds <- getDataStore(layer)
        } {
          try {
            logger.debug(s"Deleting Replay SFT $sftName from layer $layer.")
            ds.removeSchema(sftName)
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error deleting Replay SFT $sftName from layer $layer.", e)
          }
        }
      }
    case _ =>
  }

  override def reloaded(): Unit = {}

  override def handleAddEvent(event: CatalogAddEvent): Unit = {}

  override def handlePostModifyEvent(event: CatalogPostModifyEvent): Unit = {}

  override def handleModifyEvent(event: CatalogModifyEvent): Unit = {}

  private def getDataStore(layerInfo: LayerInfo): Option[DataStore] =
    for {
      fti <- GeoServerUtils.getFeatureTypeInfo(layerInfo)
      ds <- GeoServerUtils.getDataStore(fti.getStore)
    } yield {
      ds
    }
}