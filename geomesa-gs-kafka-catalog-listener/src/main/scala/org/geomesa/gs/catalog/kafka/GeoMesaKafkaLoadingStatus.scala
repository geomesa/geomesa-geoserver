/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.catalog.kafka

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import org.geoserver.platform.ModuleStatus
import org.locationtech.geomesa.kafka.data.KafkaCacheLoader

class GeoMesaKafkaLoadingStatus extends ModuleStatus {
  /**
   * Module identifier based on artifact bundle Example: <code>gs-main</code>, <code>gs-oracle
   * </code>
   */
  override def getModule: String = "geomesa-kafka-loader"

  /** Optional component identifier within module. Example: <code>rendering-engine</code> */
  override def getComponent: Optional[String] = Optional.empty()

  /**
   * Human readable name (from GeoServer documentation), or as defined in the extension pom.xml,
   * ie. <name>PostGIS DataStore Extensions</name>
   */
  override def getName: String = "Loader for the GeoMesa Kafka DataStore"

  /** Human readable version, ie. for geotools it would be 15-SNAPSHOT * */
  override def getVersion: Optional[String] = Optional.empty()

  /** Returns whether the module is available to GeoServer * */
  override def isAvailable: Boolean = KafkaCacheLoader.LoaderStatus.allLoaded()

  /** Returns whether the module is enabled in the current GeoServer configuration. * */
  override def isEnabled: Boolean = true

  /**
   * Optional status message such as what Java rendering engine is in use, or the library path if
   * the module/driver is unavailable
   */
  override def getMessage: Optional[String] = {
    import scala.collection.JavaConverters._
    val status: ConcurrentHashMap[String, String] = KafkaCacheLoader.LoaderStatus.getStatus()
    println(s"Got status: $status")
    Optional.of(status.toString)
  }

  /** Optional relative link to GeoServer user manual */
  override def getDocumentation: Optional[String] = Optional.empty()
}
