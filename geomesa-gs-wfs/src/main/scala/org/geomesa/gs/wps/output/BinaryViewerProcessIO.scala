/***********************************************************************
 * Copyright (c) 2013-2024 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wps.output

import org.geomesa.gs.wfs.output.BinaryViewerOutputFormat
import org.geoserver.wps.ppio.BinaryPPIO
import org.geoserver.wps.ppio.ProcessParameterIO.PPIODirection

import java.io.{InputStream, OutputStream}

/**
  * WPS output formatting for binary viewer. Does not support input formatting.
  */
class BinaryViewerProcessIO extends BinaryPPIO(classOf[java.util.Iterator[Array[Byte]]],
  classOf[java.util.Iterator[Array[Byte]]], BinaryViewerOutputFormat.MIME_TYPE) {

  import scala.collection.JavaConverters._

  override def getDirection: PPIODirection = PPIODirection.ENCODING

  override def encode(value: AnyRef, os: OutputStream): Unit =
    value.asInstanceOf[java.util.Iterator[Array[Byte]]].asScala.foreach(os.write)

  override def decode(input: InputStream): AnyRef = throw new NotImplementedError("Only supports encode")

  override def getFileExtension: String = ".bin"
}
