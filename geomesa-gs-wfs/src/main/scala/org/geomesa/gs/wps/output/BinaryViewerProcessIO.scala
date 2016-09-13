/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wps.output

import java.io.{InputStream, OutputStream}

import org.geomesa.gs.wfs.output.BinaryViewerOutputFormat
import org.geoserver.wps.ppio.BinaryPPIO
import org.geoserver.wps.ppio.ProcessParameterIO.PPIODirection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.FeatureCollection
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder

/**
  * WPS output formatting for binary viewer. Does not support input formatting.
  */
class BinaryViewerProcessIO extends BinaryPPIO(classOf[java.util.Iterator[Array[Byte]]],
  classOf[java.util.Iterator[Array[Byte]]], BinaryViewerOutputFormat.MIME_TYPE) {

  override def getDirection: PPIODirection = PPIODirection.ENCODING

  override def encode(value: AnyRef, os: OutputStream): Unit = {
    import scala.collection.JavaConversions._
    value.asInstanceOf[java.util.Iterator[Array[Byte]]].foreach(os.write)
  }

  override def decode(input: InputStream): AnyRef = throw new NotImplementedError("Only supports encode")

  override def getFileExtension: String = ".bin"
}

/**
  * WPS output formatting for binary viewer. Does not support input formatting.
  */
@deprecated("Use chained BinaryViewerProcess")
class BinaryViewerFeatureCollectionProcessIO extends BinaryPPIO(classOf[FeatureCollection[_, _]],
  classOf[FeatureCollection[_, _]], BinaryViewerOutputFormat.MIME_TYPE) {

  override def getDirection: PPIODirection = PPIODirection.ENCODING

  override def encode(value: AnyRef, os: OutputStream): Unit = {
    import scala.collection.JavaConversions._
    val fc = value.asInstanceOf[SimpleFeatureCollection]
    val options = BinaryOutputEncoder.CollectionEncodingOptions.remove(fc.getID)
    if (options == null) {
      throw new IllegalArgumentException("BIN process output requires configuration of BIN values in request")
    }
    BinaryOutputEncoder.encodeFeatureCollection(fc, os, options, sort = false)
  }

  override def decode(input: InputStream): AnyRef = throw new NotImplementedError("Only supports encode")

  override def getFileExtension: String = ".bin"
}
